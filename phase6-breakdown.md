# Phase 6 Breakdown: Operator

## Current State (end of Phase 5)

All tests pass. The operator binary entry point (`cmd/skafka-operator/main.go`) exists
but is empty (`func main() {}`). The four CRD types are fully defined in
`operator/api/v1alpha1/`. The `sigs.k8s.io/controller-runtime v0.23.3` is already in
`go.mod`. The broker's `--init` flag already creates partition directories from CRDs on
startup; the operator's KafkaTopic controller duplicates and extends this for
create/update/delete lifecycle.

### Key files before starting Phase 6

| File | Role |
|---|---|
| `cmd/skafka-operator/main.go` | Empty — rewritten in Step 6.0 |
| `operator/api/v1alpha1/kafkatopic_types.go` | CRD type: `KafkaTopic`, `KafkaTopicSpec`, `KafkaTopicStatus` |
| `operator/api/v1alpha1/kafkauser_types.go` | CRD type: `KafkaUser`, auth types (scram/tls/sa), quotas |
| `operator/api/v1alpha1/kafkausergroup_types.go` | CRD type: `KafkaUserGroup` with `AclRule` list |
| `operator/api/v1alpha1/kafkaacl_types.go` | CRD type: `KafkaAcl` with `AclPrincipal` + `[]AclRule` |
| `internal/storage/watcher.go` | inotify watcher — brokers pick up new credentials.json/acls.json automatically |

### New dependency needed

```bash
go get golang.org/x/crypto@latest   # for PBKDF2-HMAC-SHA-512 (SCRAM credential hashing)
```

`golang.org/x/crypto` is used by kubernetes dependencies transitively but is not
currently a direct dependency.

### Operator pod requirements

The operator pod must mount the shared PVC at `SKAFKA_DATA_DIR` to write
`__cluster/credentials.json` and `__cluster/acls.json`, and to create/remove topic
partition directories. This is set via a projected volume + env var in the Deployment
manifest (Phase 8).

---

## File layout for Phase 6

```
cmd/skafka-operator/main.go          ← rewrite: controller-runtime manager setup
operator/controllers/
  kafkatopic_controller.go           ← Step 6.1
  credentials.go                     ← Step 6.2 shared: JSON format + atomic write
  kafkauser_controller.go            ← Step 6.2
  acls.go                            ← Step 6.3/6.4 shared: JSON format + merge
  kafkausergroup_controller.go       ← Step 6.3
  kafkaacl_controller.go             ← Step 6.4
```

---

## Step 6.0 — Manager setup

File: `cmd/skafka-operator/main.go`

```go
func main() {
    cfg := ctrl.GetConfigOrDie()
    mgr, err := ctrl.NewManager(cfg, ctrl.Options{
        Scheme: scheme,
        // Health probe + metrics endpoints for liveness/readiness.
        HealthProbeBindAddress: ":8081",
        MetricsBindAddress:     ":8080",
    })

    dataDir := os.Getenv("SKAFKA_DATA_DIR")
    namespace := envOr("SKAFKA_NAMESPACE", "default")

    controllers.NewKafkaTopicReconciler(mgr.GetClient(), dataDir).SetupWithManager(mgr)
    controllers.NewKafkaUserReconciler(mgr.GetClient(), dataDir, namespace).SetupWithManager(mgr)
    controllers.NewKafkaUserGroupReconciler(mgr.GetClient(), dataDir, namespace).SetupWithManager(mgr)
    controllers.NewKafkaAclReconciler(mgr.GetClient(), dataDir, namespace).SetupWithManager(mgr)

    mgr.AddHealthzCheck("healthz", healthz.Ping)
    mgr.AddReadyzCheck("readyz", healthz.Ping)
    mgr.Start(ctrl.SetupSignalHandler())
}
```

The scheme must include both the operator CRDs (`v1alpha1.AddToScheme`) and core k8s
types (`corev1.AddToScheme`, for Secret reads/writes).

---

## Step 6.1 — KafkaTopic controller

File: `operator/controllers/kafkatopic_controller.go`

### Reconcile logic

```
1. Fetch KafkaTopic
2. If DeletionTimestamp set:
     a. Remove all partition dirs: os.RemoveAll(dataDir/{name})
     b. Remove finalizer
     c. Update object → return
3. Ensure finalizer "skafka.io/topic-cleanup" is present
4. For p in 0..spec.Partitions-1:
     os.MkdirAll(dataDir/{name}/{p}, 0755)
5. Update status:
     status.PartitionCount = spec.Partitions
     Set condition Ready=True, reason="PartitionsCreated"
```

The `os.MkdirAll` call is idempotent — safe to call on every reconcile. This means
partition count increases are handled automatically (new dirs created, old ones untouched).

Partition count decreases are intentionally NOT handled — Kafka semantics don't
support shrinking partitions. If attempted, set `Ready=False` with a clear message.

### SetupWithManager

```go
func (r *KafkaTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&v1alpha1.KafkaTopic{}).
        Complete(r)
}
```

**Done when:**
- `kubectl apply -f topic.yaml` creates partition dirs on the PVC
- `kubectl delete kafkatopic` removes the dirs (via finalizer)
- `.status.partitionCount` and `.status.conditions` are updated correctly

---

## Step 6.2 — KafkaUser controller + credentials.json

### credentials.json format

File: `operator/controllers/credentials.go`

```go
type CredentialsFile struct {
    Version int               `json:"version"`
    Users   []UserCredential  `json:"users"`
}

type UserCredential struct {
    Username string            `json:"username"`
    AuthType string            `json:"authType"` // "scram-sha-512" | "tls" | "kubernetes-serviceaccount"

    // Set when authType = "scram-sha-512"
    Scram *ScramCredential `json:"scram,omitempty"`

    // Set when authType = "tls"
    TLSCN string `json:"tlsCN,omitempty"`

    // Set when authType = "kubernetes-serviceaccount"
    ServiceAccount *SACredential `json:"serviceAccount,omitempty"`

    // Quotas (optional, from KafkaUser.spec.quotas)
    Quotas *Quotas `json:"quotas,omitempty"`
}

type ScramCredential struct {
    Salt        string `json:"salt"`        // base64-encoded 16-byte random salt
    StoredKey   string `json:"storedKey"`   // base64-encoded H(ClientKey)
    ServerKey   string `json:"serverKey"`   // base64-encoded HMAC(SaltedPw, "Server Key")
    Iterations  int    `json:"iterations"`  // default 8192
}

type SACredential struct {
    Name      string `json:"name"`
    Namespace string `json:"namespace"`
}

type Quotas struct {
    ProducerByteRate  *int64 `json:"producerByteRate,omitempty"`
    ConsumerByteRate  *int64 `json:"consumerByteRate,omitempty"`
    RequestPercentage *int32 `json:"requestPercentage,omitempty"`
}
```

Atomic write pattern (shared by all file-writing controllers):
```go
func writeAtomic(path string, v any) error {
    data, _ := json.MarshalIndent(v, "", "  ")
    tmp := path + ".tmp"
    os.WriteFile(tmp, data, 0640)
    return os.Rename(tmp, path)
}
```

Read pattern (returns empty struct when file absent):
```go
func readCredentials(path string) (*CredentialsFile, error)
func writeCredentials(path string, creds *CredentialsFile) error
```

### SCRAM-SHA-512 computation

Uses `golang.org/x/crypto/pbkdf2`:

```go
import (
    "golang.org/x/crypto/pbkdf2"
    "crypto/sha512"
    "crypto/hmac"
)

const scramIterations = 8192

func computeScram(password string) (*ScramCredential, error) {
    salt := make([]byte, 16)
    rand.Read(salt)

    saltedPw := pbkdf2.Key([]byte(password), salt, scramIterations, 64, sha512.New)

    clientKey := hmacSHA512(saltedPw, []byte("Client Key"))
    storedKey := sha512sum(clientKey)
    serverKey := hmacSHA512(saltedPw, []byte("Server Key"))

    return &ScramCredential{
        Salt:       base64.StdEncoding.EncodeToString(salt),
        StoredKey:  base64.StdEncoding.EncodeToString(storedKey),
        ServerKey:  base64.StdEncoding.EncodeToString(serverKey),
        Iterations: scramIterations,
    }, nil
}
```

The salt and HMAC chain are computed once. The output is stored — the plaintext
password never touches the PVC.

### KafkaUser reconcile logic

```
1. Fetch KafkaUser (and its DeletionTimestamp)
2. If being deleted:
     a. Remove user from credentials.json
     b. Write credentials.json atomically
     c. Remove finalizer → return
3. Ensure finalizer "skafka.io/user-cleanup"
4. Switch on spec.authentication.type:

   "scram-sha-512":
     a. Read password from spec.authentication.password.secretRef
     b. Call computeScram(password) → ScramCredential
     c. Upsert UserCredential in credentials.json (replace if exists)
     d. Write credentials atomically
     e. Create/update output Secret "{name}-kafka-credentials" with
        fields: username, password (plaintext for client use)

   "tls":
     a. TLSCN = user.Name (or from spec.authentication.certificateRef.name)
     b. Upsert UserCredential{AuthType: "tls", TLSCN: cn} in credentials.json
     c. Write credentials atomically
     d. Note: cert-manager integration deferred to Phase 7+

   "kubernetes-serviceaccount":
     a. SARef = spec.authentication.serviceAccountRef
     b. Upsert UserCredential{AuthType: "kubernetes-serviceaccount", SA: ref}
     c. Write credentials atomically
     d. No Secret needed

5. Upsert quotas if spec.quotas != nil
6. Set status condition Ready=True
```

**Important:** Always read the full credentials.json before writing — only update the
entry for this user, leave all other entries intact.

**Done when:**
- Creating a `KafkaUser` with `type: scram-sha-512` writes hashed credentials to PVC
- Deleting the user removes the entry
- SCRAM vectors match RFC 5802 test vectors

---

## Step 6.3 — KafkaUserGroup controller + shared acls.go

### acls.json format

File: `operator/controllers/acls.go`

```go
type ACLFile struct {
    Version int       `json:"version"`
    ACLs    []ACLEntry `json:"acls"`
}

type ACLEntry struct {
    Principal  string      `json:"principal"`  // "User:alice" or "Group:analytics-team"
    Resource   ACLResource `json:"resource"`
    Operations []string    `json:"operations"`
    Permission string      `json:"permission"` // "Allow" | "Deny"
}

type ACLResource struct {
    Type        string `json:"type"`
    Name        string `json:"name"`
    PatternType string `json:"patternType"`
}
```

Both the KafkaUserGroup and KafkaAcl controllers call a shared function to rebuild and
write acls.json. This is the safest approach because the final file always reflects ALL
current KafkaAcl and KafkaUserGroup objects:

```go
// reconcileACLs lists all KafkaAcl + KafkaUserGroup in the namespace, merges them
// into a single ACLFile, and writes it atomically to dataDir/__cluster/acls.json.
// Idempotent — safe to call on every reconcile from either controller.
func reconcileACLs(ctx context.Context, c client.Client,
    namespace, dataDir string) error
```

### KafkaUserGroup → ACL expansion

A `KafkaUserGroup` with:
- `members: [alice, bob]`
- `rules: [{resource: {type: topic, name: payments, patternType: literal}, operations: [Read], permission: Allow}]`

Expands to ACL entries for each member:
```
User:alice → Read → topic/payments/literal → Allow
User:bob   → Read → topic/payments/literal → Allow
```

The expansion uses the group's `spec.members` as individual `KafkaUser` references.

### KafkaUserGroup reconcile logic

```
1. Fetch KafkaUserGroup (and DeletionTimestamp)
2. If being deleted:
     a. Remove finalizer
     b. Trigger full ACL rebuild (reconcileACLs)
     c. Return
3. Ensure finalizer "skafka.io/usergroup-cleanup"
4. Call reconcileACLs — rebuilds entire file from all current objects
5. Update status:
     status.memberCount = len(spec.members)
     condition Ready=True
```

---

## Step 6.4 — KafkaAcl controller

File: `operator/controllers/kafkaacl_controller.go`

### KafkaAcl reconcile logic

```
1. Fetch KafkaAcl (and DeletionTimestamp)
2. If being deleted:
     a. Remove finalizer
     b. Trigger full ACL rebuild (reconcileACLs)
     c. Return
3. Ensure finalizer "skafka.io/acl-cleanup"
4. Call reconcileACLs — rebuilds entire file from all current objects
5. Update status:
     status.aclCount = len(spec.rules)
     condition Ready=True
```

### reconcileACLs in detail

```go
func reconcileACLs(ctx context.Context, c client.Client, namespace, dataDir string) error {
    // 1. List all KafkaAcl objects.
    var aclList v1alpha1.KafkaAclList
    c.List(ctx, &aclList, client.InNamespace(namespace))

    // 2. List all KafkaUserGroup objects.
    var groupList v1alpha1.KafkaUserGroupList
    c.List(ctx, &groupList, client.InNamespace(namespace))

    // 3. Merge into []ACLEntry.
    var entries []ACLEntry

    // From KafkaAcl objects:
    for _, acl := range aclList.Items {
        if acl.DeletionTimestamp != nil { continue }
        principal := formatPrincipal(acl.Spec.Principal)
        for _, rule := range acl.Spec.Rules {
            entries = append(entries, toACLEntry(principal, rule))
        }
    }

    // From KafkaUserGroup objects (expand members):
    for _, group := range groupList.Items {
        if group.DeletionTimestamp != nil { continue }
        for _, member := range group.Spec.Members {
            for _, rule := range group.Spec.Rules {
                entries = append(entries, toACLEntry("User:"+member, rule))
            }
        }
    }

    // 4. Ensure __cluster dir exists.
    os.MkdirAll(filepath.Join(dataDir, "__cluster"), 0755)

    // 5. Write atomically.
    path := filepath.Join(dataDir, "__cluster", "acls.json")
    return writeAtomic(path, &ACLFile{Version: 1, ACLs: entries})
}

func formatPrincipal(p v1alpha1.AclPrincipal) string {
    switch p.Kind {
    case "KafkaUser":
        return "User:" + p.Name
    case "KafkaUserGroup":
        return "Group:" + p.Name
    }
    return p.Name
}
```

Once written, the inotify watcher in `internal/storage/watcher.go` (Phase 3) fires in
all broker pods automatically — no restart required.

---

## RBAC manifests

File: `config/rbac/operator_role.yaml`

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: skafka-operator
rules:
  - apiGroups: ["skafka.io"]
    resources: ["kafkatopics", "kafkausers", "kafkausergroups", "kafkaacls"]
    verbs: ["get", "list", "watch", "update", "patch"]
  - apiGroups: ["skafka.io"]
    resources: ["kafkatopics/status", "kafkausers/status",
                "kafkausergroups/status", "kafkaacls/status",
                "kafkatopics/finalizers", "kafkausers/finalizers",
                "kafkausergroups/finalizers", "kafkaacls/finalizers"]
    verbs: ["update", "patch"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch"]
```

---

## Testing strategy

| Test | Location | Approach |
|---|---|---|
| SCRAM vector | `operator/controllers/credentials_test.go` | Fixed RFC 5802 test vectors |
| credentials.json read/write | same | temp file |
| acls.json merge | `operator/controllers/acls_test.go` | in-memory |
| KafkaTopic reconcile | `operator/controllers/kafkatopic_controller_test.go` | envtest + temp dir |
| KafkaUser SCRAM | same package | envtest + temp dir |
| ACL rebuild from KafkaAcl + KafkaUserGroup | same | envtest |

For envtest: use `sigs.k8s.io/controller-runtime/pkg/envtest` which is already cached.
The CRD YAML must be generated from the type annotations before tests run:
```bash
make manifests   # generates config/crd/bases/*.yaml
```
Or point `envtest.Environment.CRDDirectoryPaths` at the type source and use
`envtest.CRDInstallOptions` with `controller-gen` output.

---

## Step order summary

| Step | File(s) | Depends on |
|---|---|---|
| 6.0 Manager setup | `cmd/skafka-operator/main.go` | nothing |
| 6.1 KafkaTopic | `controllers/kafkatopic_controller.go` | 6.0 |
| 6.2a credentials.go | `controllers/credentials.go` | nothing |
| 6.2b KafkaUser | `controllers/kafkauser_controller.go` | 6.0, 6.2a |
| 6.3a acls.go | `controllers/acls.go` | nothing |
| 6.3b KafkaUserGroup | `controllers/kafkausergroup_controller.go` | 6.0, 6.3a |
| 6.4 KafkaAcl | `controllers/kafkaacl_controller.go` | 6.0, 6.3a |

Steps 6.1, 6.2a, and 6.3a are fully independent and can be done in parallel.
Steps 6.2b, 6.3b, and 6.4 can also run in parallel once their dependencies are done.
