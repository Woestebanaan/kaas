package controllers

import (
	"context"
	"crypto/rand"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// KafkaUserReconciler manages user credentials on the shared PVC.
type KafkaUserReconciler struct {
	client.Client
	DataDir   string
	Namespace string
}

func NewKafkaUserReconciler(c client.Client, dataDir, namespace string) *KafkaUserReconciler {
	return &KafkaUserReconciler{Client: c, DataDir: dataDir, Namespace: namespace}
}

func (r *KafkaUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaUser{}).
		Complete(Observed("KafkaUser", r))
}

func (r *KafkaUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var user v1alpha1.KafkaUser
	err := r.Get(ctx, req.NamespacedName, &user)
	if apierrors.IsNotFound(err) {
		// CR is gone — drop the credential entry and rebuild acls.json
		// without any rules that belonged to this user. If the operator
		// was down during the delete, the startup credentials sweep
		// catches the orphan credential; acls.json gets rebuilt from
		// the live KafkaUser set on next reconcile.
		if e := r.removeUser(req.Name); e != nil {
			return ctrl.Result{}, e
		}
		if e := reconcileACLs(ctx, r.Client, r.Namespace, r.DataDir); e != nil {
			return ctrl.Result{}, e
		}
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if !user.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	cred, secretName, err := r.buildCredential(ctx, &user)
	if err != nil {
		setCondition(&user.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "CredentialError",
			Message: err.Error(),
		})
		_ = r.Status().Update(ctx, &user)
		return ctrl.Result{}, err
	}

	cf, err := readCredentials(r.DataDir)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("read credentials: %w", err)
	}
	cf.upsertUser(*cred)
	if err := writeCredentials(r.DataDir, cf); err != nil {
		return ctrl.Result{}, fmt.Errorf("write credentials: %w", err)
	}

	// gh #135: ACLs now live inline on the KafkaUser CR. Rebuild acls.json
	// from every user's spec.authorization.acls on each reconcile so a
	// single-user edit propagates within one cycle. Cheap — the file is
	// small (<1MB) and atomic-rename means the broker never sees a
	// partial write.
	if err := reconcileACLs(ctx, r.Client, r.Namespace, r.DataDir); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile acls: %w", err)
	}

	// Update status.
	user.Status.Secret = secretName
	setCondition(&user.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "CredentialWritten",
		Message: fmt.Sprintf("credentials written for %s (%s)", user.Name, user.Spec.Authentication.Type),
	})
	return ctrl.Result{}, r.Status().Update(ctx, &user)
}

// buildCredential derives the UserCredential and (for SCRAM) creates the output Secret.
func (r *KafkaUserReconciler) buildCredential(ctx context.Context, user *v1alpha1.KafkaUser) (*UserCredential, string, error) {
	auth := user.Spec.Authentication
	cred := &UserCredential{Username: user.Name, AuthType: auth.Type}

	if user.Spec.Quotas != nil {
		cred.Quotas = &CredQuotas{
			ProducerMaxByteRatePerBroker: user.Spec.Quotas.ProducerMaxByteRatePerBroker,
			ConsumerMaxByteRatePerBroker: user.Spec.Quotas.ConsumerMaxByteRatePerBroker,
			RequestPercentage:            user.Spec.Quotas.RequestPercentage,
		}
	}

	switch auth.Type {
	case "scram-sha-512":
		// gh #104: Scram field takes precedence over Password. The
		// runtime-rotation path (AlterUserScramCredentials, KIP-554)
		// writes pre-derived (salt, storedKey, serverKey, iterations)
		// directly to spec.authentication.scram. When present, we
		// pass it through to credentials.json verbatim and skip the
		// Password → PBKDF2 derivation — the broker already did the
		// derivation from the wire-level SaltedPassword. No client
		// Secret update either: the rotator already knows the new
		// password locally (Apache's model).
		if auth.Scram != nil {
			cred.Scram = &ScramCredential{
				Salt:       auth.Scram.Salt,
				StoredKey:  auth.Scram.StoredKey,
				ServerKey:  auth.Scram.ServerKey,
				Iterations: auth.Scram.Iterations,
			}
			return cred, "", nil
		}
		// gh #136: password Secret is now optional. Behaviour:
		//   - auth.Password != nil → read from the user-supplied input
		//     Secret (current behaviour; useful when an external
		//     password manager / SealedSecrets / ExternalSecrets owns
		//     the credential).
		//   - auth.Password == nil → generate a 32-char alphanumeric
		//     password on first reconcile and write it to the operator-
		//     owned output Secret (<user>-kafka-credentials). On
		//     subsequent reconciles we read the password back from
		//     that Secret so it stays stable across operator restarts.
		//     Mirrors Strimzi: "When KafkaUser.spec.authentication.type
		//     is configured with scram-sha-512 the User Operator will
		//     generate a random 32-character password consisting of
		//     upper and lowercase ASCII letters and numbers."
		outSecretName := user.Name + "-kafka-credentials"
		password, err := r.resolveSCRAMPassword(ctx, user, outSecretName)
		if err != nil {
			return nil, "", err
		}
		scram, err := computeScram(password)
		if err != nil {
			return nil, "", fmt.Errorf("compute scram: %w", err)
		}
		cred.Scram = scram

		if err := r.ensureClientSecret(ctx, user, outSecretName, user.Name, password); err != nil {
			return nil, "", fmt.Errorf("create client secret: %w", err)
		}
		return cred, outSecretName, nil

	case "tls":
		cn := user.Name
		if auth.CertificateRef != nil && auth.CertificateRef.Name != "" {
			cn = auth.CertificateRef.Name
		}
		cred.TLSCN = cn
		return cred, "", nil

	case "kubernetes-serviceaccount":
		if auth.ServiceAccountRef == nil {
			return nil, "", fmt.Errorf("spec.authentication.serviceAccountRef required for kubernetes-serviceaccount")
		}
		cred.SA = &SACredential{
			Name:      auth.ServiceAccountRef.Name,
			Namespace: auth.ServiceAccountRef.Namespace,
		}
		return cred, "", nil

	default:
		return nil, "", fmt.Errorf("unsupported authentication type: %q", auth.Type)
	}
}

func (r *KafkaUserReconciler) readSecret(ctx context.Context, namespace, name, key string) (string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &secret); err != nil {
		return "", err
	}
	val, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s", key, name)
	}
	return string(val), nil
}

// resolveSCRAMPassword returns the password for a scram-sha-512 user.
//
// If the CR specifies spec.authentication.password, the input Secret
// is the source of truth (external-secrets / SealedSecrets pattern).
// Otherwise the operator generates a 32-char alphanumeric password
// on first reconcile and persists it to the output Secret; subsequent
// reconciles read it back from there so the password stays stable
// across operator restarts. Mirrors Strimzi's User Operator behaviour.
// gh #136.
func (r *KafkaUserReconciler) resolveSCRAMPassword(ctx context.Context, user *v1alpha1.KafkaUser, outSecretName string) (string, error) {
	if user.Spec.Authentication.Password != nil {
		ref := user.Spec.Authentication.Password
		password, err := r.readSecret(ctx, user.Namespace, ref.Name, ref.Key)
		if err != nil {
			return "", fmt.Errorf("read password secret: %w", err)
		}
		return password, nil
	}

	// Auto-generate path. Try to read the existing output Secret first
	// so we don't churn the password (and the downstream credentials.json
	// scram hash) on every reconcile.
	var outSecret corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Namespace: user.Namespace, Name: outSecretName}, &outSecret)
	if err == nil {
		if pw, ok := outSecret.Data["password"]; ok && len(pw) > 0 {
			return string(pw), nil
		}
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("read existing output secret: %w", err)
	}

	password, err := generateAlphaNumPassword(32)
	if err != nil {
		return "", fmt.Errorf("generate password: %w", err)
	}
	return password, nil
}

// generateAlphaNumPassword returns a string of n characters drawn
// uniformly (with negligible modulo bias for 32×62≈190 bits of
// entropy) from [A-Za-z0-9]. Matches Strimzi's password alphabet.
func generateAlphaNumPassword(n int) (string, error) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	out := make([]byte, n)
	for i, b := range raw {
		out[i] = alphabet[int(b)%len(alphabet)]
	}
	return string(out), nil
}

func (r *KafkaUserReconciler) ensureClientSecret(ctx context.Context, owner *v1alpha1.KafkaUser, secretName, username, password string) error {
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: owner.Namespace,
		},
		StringData: map[string]string{
			"username": username,
			"password": password,
		},
	}

	var existing corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Namespace: owner.Namespace, Name: secretName}, &existing)
	if apierrors.IsNotFound(err) {
		_ = controllerutil.SetControllerReference(owner, desired, r.Scheme())
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.StringData = desired.StringData
	return r.Update(ctx, &existing)
}

// removeUser drops one entry from credentials.json. Used as a fast path on
// Reconcile-time deletion; the startup sweep does the same thing for orphans.
func (r *KafkaUserReconciler) removeUser(username string) error {
	cf, err := readCredentials(r.DataDir)
	if err != nil {
		return err
	}
	cf.removeUser(username)
	return writeCredentials(r.DataDir, cf)
}

// SweepCredentials rebuilds credentials.json from all current KafkaUser CRs in
// namespace, dropping any entries whose CR is gone. Called once at operator
// startup. Unlike the per-user reconcile, this only rewrites entries we can
// reconstitute without re-reading password Secrets — for SCRAM users the
// existing entry is preserved if the CR still exists, since recomputing the
// scram hash would change salt/keys on every restart.
func SweepCredentials(ctx context.Context, c client.Client, namespace, dataDir string) ([]string, error) {
	var users v1alpha1.KafkaUserList
	if err := c.List(ctx, &users, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list KafkaUsers: %w", err)
	}
	keep := map[string]bool{}
	for _, u := range users.Items {
		keep[u.Name] = true
	}

	cf, err := readCredentials(dataDir)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}

	var removed []string
	out := cf.Users[:0]
	for _, u := range cf.Users {
		if keep[u.Username] {
			out = append(out, u)
			continue
		}
		removed = append(removed, u.Username)
	}
	if len(removed) == 0 {
		return nil, nil
	}
	cf.Users = out
	if err := writeCredentials(dataDir, cf); err != nil {
		return removed, fmt.Errorf("write credentials: %w", err)
	}
	return removed, nil
}
