package controllers

import (
	"context"
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
		Complete(r)
}

func (r *KafkaUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var user v1alpha1.KafkaUser
	err := r.Get(ctx, req.NamespacedName, &user)
	if apierrors.IsNotFound(err) {
		// CR is gone — drop the credential entry. If the operator was down
		// during the delete, the startup credentials rebuild catches it.
		if e := r.removeUser(req.Name); e != nil {
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
			ProducerByteRate:  user.Spec.Quotas.ProducerByteRate,
			ConsumerByteRate:  user.Spec.Quotas.ConsumerByteRate,
			RequestPercentage: user.Spec.Quotas.RequestPercentage,
		}
	}

	switch auth.Type {
	case "scram-sha-512":
		if auth.Password == nil {
			return nil, "", fmt.Errorf("spec.authentication.password required for scram-sha-512")
		}
		password, err := r.readSecret(ctx, user.Namespace, auth.Password.Name, auth.Password.Key)
		if err != nil {
			return nil, "", fmt.Errorf("read password secret: %w", err)
		}
		scram, err := computeScram(password)
		if err != nil {
			return nil, "", fmt.Errorf("compute scram: %w", err)
		}
		cred.Scram = scram

		// Create/update an output Secret for the client application.
		outSecretName := user.Name + "-kafka-credentials"
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
