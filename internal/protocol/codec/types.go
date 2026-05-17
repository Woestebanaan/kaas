// Package codec encodes/decodes Kafka request and response *frames* — i.e.,
// the fixed-size headers and primitive types that wrap each API call. It
// deliberately does NOT contain a decoded RecordBatch type or any per-record
// helpers; per v3.3 plan constraint #22, RecordBatch payloads are byte-opaque
// throughout the broker. Tests that need a decoded representation import
// tests/testutil/recordbatch.
package codec

// ErrorCode is a typed Kafka error code (int16).
type ErrorCode int16

const (
	ErrNone                        ErrorCode = 0
	ErrUnknownServerError          ErrorCode = -1
	ErrOffsetOutOfRange            ErrorCode = 1
	ErrCorruptMessage              ErrorCode = 2
	ErrUnknownTopicOrPartition     ErrorCode = 3
	ErrLeaderNotAvailable          ErrorCode = 5
	ErrNotLeaderOrFollower         ErrorCode = 6
	ErrRequestTimedOut             ErrorCode = 7
	ErrNetworkException            ErrorCode = 13
	ErrCoordinatorNotAvailable     ErrorCode = 15
	ErrNotCoordinator              ErrorCode = 16
	ErrInvalidTopicException       ErrorCode = 17
	ErrMessageTooLarge             ErrorCode = 18
	ErrGroupLoadInProgress         ErrorCode = 14
	ErrIllegalGeneration           ErrorCode = 22
	ErrInconsistentGroupProtocol   ErrorCode = 23
	ErrUnknownMemberId             ErrorCode = 25
	ErrInvalidSessionTimeout       ErrorCode = 26
	ErrRebalanceInProgress         ErrorCode = 27
	ErrTopicAuthorizationFailed    ErrorCode = 29
	ErrGroupAuthorizationFailed    ErrorCode = 30
	// ErrClusterAuthorizationFailed (31) — principal lacks the
	// requested Cluster-scoped permission. AdminClient surfaces it
	// as ClusterAuthorizationException. Used by DescribeClientQuotas
	// / AlterClientQuotas (gh #103) and other cluster-admin APIs.
	ErrClusterAuthorizationFailed  ErrorCode = 31
	ErrUnsupportedSaslMechanism    ErrorCode = 33
	ErrUnsupportedVersion          ErrorCode = 35
	ErrTopicAlreadyExists          ErrorCode = 36
	ErrInvalidPartitions           ErrorCode = 37
	ErrInvalidRequest              ErrorCode = 42
	ErrOutOfOrderSequenceNumber    ErrorCode = 45
	ErrDuplicateSequenceNumber     ErrorCode = 46
	ErrInvalidProducerEpoch        ErrorCode = 47
	// ErrInvalidProducerIDMapping (49) is what
	// AddPartitionsToTransaction returns when the (transactionalID,
	// PID) tuple is unknown to the coordinator — either no
	// InitProducerId has been seen, or the PID in the request
	// doesn't match the persisted entry. gh #23.
	// ErrInvalidGroupID (24) — TxnOffsetCommit / AddOffsetsToTxn
	// reject an empty groupID at the wire level. Distinct from
	// ErrGroupIDNotFound which signals "this group is unknown to
	// the coordinator" (a state-level error).
	ErrInvalidGroupID              ErrorCode = 24
	ErrInvalidProducerIDMapping    ErrorCode = 49
	// ErrInvalidTxnState (50) is what EndTxn returns when the
	// requested transition isn't legal from the current state — e.g.,
	// abort against a CompleteCommit entry, or any EndTxn against
	// an Empty entry. gh #25/#26.
	ErrInvalidTxnState             ErrorCode = 50
	// ErrConcurrentTransactions (51) is what AddPartitionsToTxn and
	// EndTxn return when another transition for the same txnID is
	// already in flight. Retriable; client backs off and re-sends.
	// gh #25.
	ErrConcurrentTransactions      ErrorCode = 51
	ErrTransactionalIdAuthFailed   ErrorCode = 53
	ErrNonEmptyGroup               ErrorCode = 67
	ErrGroupIDNotFound             ErrorCode = 69
	ErrCoordinatorLoadInProgress   ErrorCode = 14

	// ErrMemberIDRequired pairs with KIP-394 (Apache Kafka 2.3+).
	// The coordinator returns this on the FIRST JoinGroup from a
	// dynamic member (no GroupInstanceID, empty member.id, request
	// version >= 4): the response carries an assigned member.id and
	// this error code, telling the client to retry the JoinGroup
	// using the new ID. The retry counts toward the rebalance — the
	// initial request does not. Used to fence "zombie" members that
	// reconnected with no memberID after a network blip.
	ErrMemberIDRequired ErrorCode = 79

	// ErrProducerFenced (90) is returned when an InitProducerId or
	// AddPartitionsToTransaction arrives with an epoch that doesn't
	// match the coordinator's stored entry — i.e. another session
	// of the same transactional.id has bumped the epoch. The
	// fenced producer must call initTransactions() (which
	// re-allocates a fresh epoch) or surface the error to the
	// application. Apache Kafka's `Errors.PRODUCER_FENCED`. gh #23.
	ErrProducerFenced ErrorCode = 90
)

