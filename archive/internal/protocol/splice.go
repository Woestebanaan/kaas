package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// Splicer is the abstraction Fetch (and any future splice-friendly
// handler) uses to interleave byte chunks with file slices when writing
// a response. Plaintext TCP connections take a kernel-side splice via
// sendfile(2); TLS / non-TCP connections fall back to a userspace
// ReadAt + Write copy that goes through the connection's existing
// encrypt path. Either way the caller's response shape is unchanged —
// the difference is whether the records bytes ever cross userspace.
//
// gh #130: introduced as part of the sendfile-on-Fetch work. Not yet
// wired into the Fetch handler — that's the next PR.
type Splicer interface {
	// Write enqueues bytes onto the response stream. Returns the number
	// of bytes consumed and an error if the underlying writer faults.
	Write(p []byte) (int, error)

	// Splice writes `length` bytes starting at `offset` from `file`
	// onto the response stream. Implementations choose between
	// sendfile(2) (no userspace copy) and a ReadAt/Write fallback
	// based on the connection type at construction time.
	//
	// Caller MUST NOT Close file — its lifecycle is owned by the
	// storage engine. A concurrent Relinquish that closes the file
	// will surface as EBADF here; callers should treat that the same
	// as any other transient I/O error (return to the client, who
	// retries via the Kafka protocol's NOT_LEADER_FOR_PARTITION
	// contract).
	Splice(file *os.File, offset int64, length int) error

	// Flush forces any buffered bytes onto the wire. Splice may need
	// to flush its predecessor byte chunks before calling sendfile
	// (we can't sendfile through a bufio.Writer's pending buffer).
	Flush() error

	// IsKernelSplice reports whether Splice actually avoids the
	// userspace copy (true for plaintext *net.TCPConn via sendfile,
	// false for the TLS / generic-net.Conn copy-fallback path).
	// Splicing-aware handlers use this to decide whether the splice
	// path is worth the extra encoder cooperation — on the copy path
	// they get the same wire output as the standard []byte route at
	// the cost of an extra ReadAt, so it's only worth it on the
	// kernel-splice path.
	IsKernelSplice() bool

	// Cork / Uncork are batching hints to the kernel. Between paired
	// calls, the implementation MAY accumulate output without sending
	// (TCP_CORK on Linux for tcpSplicer; no-op for copySplicer). The
	// Splicing handler brackets each Fetch response in Cork/Uncork so
	// the alternating Write(header) + Splice(records) pattern doesn't
	// produce one small TCP segment per header chunk.
	Cork()
	Uncork()
}

// NewSplicerFor picks the right Splicer for a connection. Plaintext
// *net.TCPConn → tcpSplicer (sendfile). Anything else (incl. TLS
// connections, which wrap an underlying net.Conn and don't surface a
// raw TCPConn) → copySplicer.
func NewSplicerFor(conn net.Conn, bw *bufio.Writer) Splicer {
	if tcp, ok := conn.(*net.TCPConn); ok {
		return &tcpSplicer{bw: bw, conn: tcp}
	}
	return &copySplicer{bw: bw}
}

// --- TCPSplicer: sendfile path ---

type tcpSplicer struct {
	bw   *bufio.Writer
	conn *net.TCPConn
}

func (t *tcpSplicer) Write(p []byte) (int, error) {
	return t.bw.Write(p)
}

func (t *tcpSplicer) Flush() error {
	return t.bw.Flush()
}

func (t *tcpSplicer) IsKernelSplice() bool { return true }

// Cork / Uncork toggle TCP_CORK on the underlying socket. Between
// Cork() and Uncork() the kernel queues outbound bytes locally instead
// of pushing them out as soon as they fit in a TCP segment; the
// uncork either coalesces everything into one large MTU-filling
// burst, or sends now if a full segment is already queued. Apache
// Kafka's NIO transferTo path gets the same batching implicitly from
// the JVM's buffered write semantics — we have to be explicit.
//
// Safe on errors: if the setsockopt fails (older kernel, non-TCP
// socket somehow), we just don't cork. No correctness impact, only
// throughput.
func (t *tcpSplicer) Cork() {
	rc, err := t.conn.SyscallConn()
	if err != nil {
		return
	}
	_ = rc.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_CORK, 1)
	})
}

func (t *tcpSplicer) Uncork() {
	rc, err := t.conn.SyscallConn()
	if err != nil {
		return
	}
	_ = rc.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_CORK, 0)
	})
}

func (t *tcpSplicer) Splice(file *os.File, offset int64, length int) error {
	if file == nil {
		return fmt.Errorf("tcpSplicer: nil file")
	}
	if length <= 0 {
		return nil
	}
	// sendfile pulls bytes from the kernel page cache and pushes them
	// straight onto the socket. We MUST flush the bufio.Writer first
	// so the records bytes land after any byte-chunks the handler
	// queued via Write — sendfile bypasses bw.
	if err := t.bw.Flush(); err != nil {
		return fmt.Errorf("tcpSplicer: flush before sendfile: %w", err)
	}
	rc, err := t.conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("tcpSplicer: SyscallConn: %w", err)
	}
	off := offset
	remaining := length
	var sendErr error
	// Use Write, not Control: Control runs fn once without netpoll
	// integration, so EAGAIN (TCP send buffer full) is the caller's
	// problem to retry — and the only way to wait for writability is
	// to either spin (bad) or go through netpoll, which Control
	// doesn't expose. Write blocks via the runtime poller until the
	// socket is writable, then invokes fn; if fn returns false, Write
	// re-blocks and retries. That's exactly the loop sendfile needs.
	writeErr := rc.Write(func(outFd uintptr) (done bool) {
		n, e := unix.Sendfile(int(outFd), int(file.Fd()), &off, remaining)
		if n > 0 {
			remaining -= n
		}
		switch e {
		case nil:
			return remaining == 0
		case unix.EAGAIN:
			// Send buffer full. Return false so rc.Write re-blocks
			// on the netpoller until the socket is writable again,
			// then invokes us with the same fd. off has advanced
			// for partial bytes, so we resume cleanly.
			return false
		case unix.EINTR:
			// Interrupted by a signal before any progress; retry.
			return false
		default:
			sendErr = fmt.Errorf("sendfile: %w", e)
			return true
		}
	})
	if writeErr != nil {
		return fmt.Errorf("tcpSplicer: SyscallConn.Write: %w", writeErr)
	}
	return sendErr
}

// --- CopySplicer: TLS / fallback path ---

type copySplicer struct {
	bw *bufio.Writer
}

func (c *copySplicer) Write(p []byte) (int, error) {
	return c.bw.Write(p)
}

func (c *copySplicer) Flush() error {
	return c.bw.Flush()
}

func (c *copySplicer) IsKernelSplice() bool { return false }
func (c *copySplicer) Cork()                {}
func (c *copySplicer) Uncork()              {}

// copySpliceChunkSize is the read-buffer size for the ReadAt → Write
// loop. 64 KiB matches our bufio.Writer's default buffer in serveConn,
// so each iteration roughly fills + flushes one buffer.
const copySpliceChunkSize = 64 * 1024

func (c *copySplicer) Splice(file *os.File, offset int64, length int) error {
	if file == nil {
		return fmt.Errorf("copySplicer: nil file")
	}
	if length <= 0 {
		return nil
	}
	buf := make([]byte, copySpliceChunkSize)
	off := offset
	remaining := length
	for remaining > 0 {
		want := remaining
		if want > len(buf) {
			want = len(buf)
		}
		n, err := file.ReadAt(buf[:want], off)
		if n > 0 {
			if _, werr := c.bw.Write(buf[:n]); werr != nil {
				return fmt.Errorf("copySplicer: write: %w", werr)
			}
			off += int64(n)
			remaining -= n
		}
		if err != nil {
			// EOF mid-splice means our caller asked for more bytes than
			// the segment file actually has. The Kafka wire allows
			// truncated tails of Fetch responses (clients discard
			// incomplete final batches), so we surface that with a
			// distinguishable error and let the handler decide.
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return fmt.Errorf("copySplicer: read: %w", err)
		}
	}
	return nil
}
