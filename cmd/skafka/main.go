package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/storage"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	host := envOr("SKAFKA_HOST", "0.0.0.0")
	port := envOr("SKAFKA_PORT", "9092")
	clusterID := envOr("SKAFKA_CLUSTER_ID", "skafka-local")
	dataDir := os.Getenv("SKAFKA_DATA_DIR")

	leases := broker.NewLocalLeaseManager()
	locks := broker.NewLocalPartitionLock()

	var store storage.StorageEngine
	if dataDir != "" {
		engine, err := storage.NewDiskStorageEngine(dataDir, leases, lock.NewFlockLock(dataDir), storage.DefaultConfig())
		if err != nil {
			slog.Error("failed to open disk storage", "dir", dataDir, "err", err)
			os.Exit(1)
		}
		store = engine
		slog.Info("using disk storage", "dir", dataDir)
	} else {
		store = broker.NewMemoryStorage()
		slog.Info("using in-memory storage (data will be lost on restart)")
	}

	b := broker.New(
		broker.Config{
			BrokerID:  0,
			Host:      host,
			Port:      9092,
			ClusterID: clusterID,
		},
		store,
		leases,
		locks,
		broker.NewAllowAllAuthEngine(),
	)

	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	srv := protocol.NewServer(protocol.Config{ListenAddr: host + ":" + port}, d)
	if err := srv.Start(ctx); err != nil {
		slog.Error("failed to start server", "err", err)
		os.Exit(1)
	}

	slog.Info("skafka broker ready", "host", host, "port", port, "cluster_id", clusterID)
	<-ctx.Done()
	slog.Info("shutting down")
	srv.Wait()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
