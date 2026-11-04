/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	internal "github.com/linode/cloud-firewall-controller/internal/types"
)

var _ = Describe("createLinodeClient", func() {
	opts := internal.LinodeApiOptions{Credentials: "linode", CredentialsNs: "kube-system"}
	secretsForbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "", errors.New("the cached client must not read Secrets"))

	// The cached client refuses all Secret reads, so the credentials must come from APIReader.
	cachedClient := func() client.Client {
		return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return secretsForbidden
				}
				return c.Get(ctx, key, obj, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.SecretList); ok {
					return secretsForbidden
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
	}

	reconcilerWith := func(objs ...client.Object) *CloudFirewallReconciler {
		return &CloudFirewallReconciler{
			Client:    cachedClient(),
			APIReader: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build(),
			Scheme:    scheme.Scheme,
		}
	}

	secret := func(data map[string][]byte) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: opts.Credentials, Namespace: opts.CredentialsNs},
			Data:       data,
		}
	}

	It("reads the token through APIReader, not the cached client", func() {
		r := reconcilerWith(secret(map[string][]byte{"token": []byte("test-token")}))
		Expect(r.createLinodeClient(context.Background(), opts)).To(Succeed())
	})

	It("returns an error when the Secret has no token", func() {
		r := reconcilerWith(secret(map[string][]byte{"other": []byte("x")}))
		Expect(r.createLinodeClient(context.Background(), opts)).To(MatchError(ContainSubstring("failed to parse Linode API token")))
	})

	It("returns an error when the Secret does not exist", func() {
		r := reconcilerWith()
		Expect(r.createLinodeClient(context.Background(), opts)).To(MatchError(ContainSubstring("not found")))
	})

	Context("when the API server does not answer", func() {
		type getCall struct {
			deadline    time.Time
			hasDeadline bool
		}

		// The APIReader records the deadline of the context it receives and then blocks
		// until that context is done, like a GET to an API server that never answers.
		blockingReconciler := func(calls chan<- getCall) *CloudFirewallReconciler {
			return &CloudFirewallReconciler{
				Client: cachedClient(),
				APIReader: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
						deadline, ok := ctx.Deadline()
						calls <- getCall{deadline: deadline, hasDeadline: ok}
						<-ctx.Done()
						return ctx.Err()
					},
				}).Build(),
				Scheme: scheme.Scheme,
			}
		}

		// Run createLinodeClient in a goroutine, so that a regression fails the spec
		// instead of hanging the suite.
		callAsync := func(ctx context.Context, r *CloudFirewallReconciler) <-chan error {
			result := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				result <- r.createLinodeClient(ctx, opts)
			}()
			return result
		}

		It("reads the Secret with a context that has a deadline of about 30s", func() {
			calls := make(chan getCall, 1)
			// The caller's context has no deadline, so any deadline comes from createLinodeClient.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			start := time.Now()
			result := callAsync(ctx, blockingReconciler(calls))

			var call getCall
			Eventually(calls).WithTimeout(5 * time.Second).Should(Receive(&call))
			Expect(call.hasDeadline).To(BeTrue(), "the credentials read must have a deadline")
			Expect(call.deadline).To(BeTemporally("~", start.Add(30*time.Second), time.Second), "the credentials read must time out after about 30s")

			cancel()
			Eventually(result).WithTimeout(5 * time.Second).Should(Receive(HaveOccurred()))
		})

		It("returns an error when the caller's context ends", func() {
			calls := make(chan getCall, 1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			result := callAsync(ctx, blockingReconciler(calls))

			var err error
			Eventually(result).WithTimeout(5 * time.Second).Should(Receive(&err))
			Expect(err).To(MatchError(ContainSubstring(context.DeadlineExceeded.Error())))
		})
	})
})
