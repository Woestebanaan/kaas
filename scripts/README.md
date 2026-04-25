# Smoke test

You (claude-code) are running inside a pod in a k3s cluster. In the same cluster skafka is also installed. The config you can find here: /home/coder/repos/k3s-cluster/apps/skafka

Can you preform a smoke test? If you encounter errors then try to fix them. And commit / release a new version?

## Prereq: a topic exists

```bash
kubectl apply -f - <<'EOF'
apiVersion: skafka.io/v1alpha1
kind: KafkaTopic
metadata: { name: smoke, namespace: skafka }
spec: { partitions: 3 }
EOF
```

The broker only picks up topics on startup — restart it after creating one:
`kubectl -n skafka rollout restart statefulset/skafka`.

## Run

```bash
./scripts/smoke-test.sh
TOPIC=demo MESSAGE='hi' ./scripts/smoke-test.sh
BOOTSTRAP=localhost:19092 ./scripts/smoke-test.sh   # via port-forward
```

Defaults: `BOOTSTRAP=skafka.skafka.svc.cluster.local:9092`, `TOPIC=smoke`,
`MESSAGE='Hello world'`. Uses `kafka-console-{producer,consumer}.sh` from
`/opt/kafka/bin`.
