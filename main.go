package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"time"

	cli "github.com/urfave/cli/v2"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	ManagerKey = "app.kubernetes.io/managed-by"
	AppName    = "awsregistry"
)

var (
	clientset  *kubernetes.Clientset
	secretName string
	namespaces cli.StringSlice
	selectors  cli.StringSlice

	lock        sync.RWMutex
	credentials map[string]string = map[string]string{}
)

func main() {
	app := &cli.App{
		Name: AppName,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name: "in-cluster",
			},
			&cli.PathFlag{
				Name:      "kubeconfig",
				Value:     "~/.kube/config",
				EnvVars:   []string{"KUBECONFIG"},
				TakesFile: true,
			},
			&cli.DurationFlag{
				Name:    "interval",
				Value:   time.Hour,
				EnvVars: []string{"INTERVAL"},
			},
			&cli.StringFlag{
				Name:        "secret-name",
				Value:       "aws-registry-credentials",
				Destination: &secretName,
				EnvVars:     []string{"SECRET_NAME"},
			},
			&cli.StringSliceFlag{
				Name:        "namespaces",
				Destination: &namespaces,
				EnvVars:     []string{"NAMESPACES"},
			},
			&cli.StringSliceFlag{
				Name:        "selector",
				Destination: &selectors,
				EnvVars:     []string{"SELECTOR"},
			},
			&cli.StringFlag{
				Name:    "aws-region",
				EnvVars: []string{"AWS_REGION"},
			},
			&cli.StringFlag{
				Name:    "aws-account-id",
				EnvVars: []string{"AWS_ACCOUNT_ID"},
			},
		},
		Before: func(ctx *cli.Context) error {
			var (
				config *rest.Config
				err error
			)

			if ctx.Bool("in-cluster") {
				if config, err = rest.InClusterConfig(); err != nil {
					return cli.Exit(
						fmt.Sprintf("failed to get in cluster config: %s", err),
						1,
					)
				}
			} else {
				if config, err = clientcmd.BuildConfigFromFlags("", ctx.Path("kubeconfig")); err != nil {
					return cli.Exit(
						fmt.Sprintf("failed to get kubernetes config: %s", err),
						1,
					)
				}
			}
			clientset, err = kubernetes.NewForConfig(config)
			if err != nil {
				return cli.Exit(
					fmt.Sprintf("failed to create clientset: %s", err),
					1,
				)
			}
			if err := updateCredentials(ctx); err != nil {
				return cli.Exit(
					fmt.Sprintf("failed to get aws credentials: %s", err),
					1,
				)
			}
			return nil
		},
		Action: func(ctx *cli.Context) error {
			go func() {
				ticker := time.NewTicker(ctx.Duration("interval"))
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						if err := updateCredentials(ctx); err != nil {
							slog.ErrorContext(ctx.Context, "failed to update credentials", "err", err)
						} else if err := syncNamespaces(ctx.Context); err != nil {
							slog.ErrorContext(ctx.Context, "failed to sync namespaces", "err", err)
						}
					case <-ctx.Done():
						return
					}
				}
			}()

			if err := watchNamespaces(ctx.Context); err != nil {
				return cli.Exit(
					fmt.Sprintf("failed to watch namespaces: %s", err),
					2,
				)
			}
			return nil
		},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Println(err.Error())
		if exit, ok := err.(cli.ExitCoder); ok {
			os.Exit(exit.ExitCode())
		}
		os.Exit(1)
	}
}

func watchNamespaces(ctx context.Context) error {
	list, err := clientset.CoreV1().Namespaces().Watch(ctx, metav1.ListOptions{})
	if err != nil {
		slog.ErrorContext(ctx, "failed to watch namespaces", "err", err)
		return err
	}
	for event := range list.ResultChan() {
		if namespace, ok := event.Object.(*corev1.Namespace); ok {
			if !isMatching(namespace) {
				continue
			}
			if event.Type == watch.Added {
				if err := syncSecret(ctx, namespace.Name); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func syncNamespaces(ctx context.Context) error {

	list, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		slog.ErrorContext(ctx, "failed to get namespaces", "err", err)
		return err
	}
	errs := make([]error, 0)
	for _, namespace := range list.Items {
		if !isMatching(&namespace) {
			continue
		}
		if err := syncSecret(ctx, namespace.Name); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func syncSecret(ctx context.Context, namespace string) error {

	secrets := clientset.CoreV1().Secrets(namespace)
	log := slog.With(
		"secretName", secretName,
		"namespace", namespace,
	)
	log.InfoContext(ctx, "syncing secret")
	create := false
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		create = true
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: namespace,
				Labels: map[string]string{
					ManagerKey: AppName,
				},
			},
			Type: "kubernetes.io/dockerconfigjson",
		}
	} else if err != nil {
		log.ErrorContext(ctx, "failed to get secret", "err", err)
		return err
	}
	if manager := secret.Labels[ManagerKey]; manager != AppName {
		log.WarnContext(ctx, "secret is managed by other application")
		return nil
	}

	lock.RLock()
	secret.Data = nil
	secret.StringData = credentials
	lock.RUnlock()

	if create {
		log.InfoContext(ctx, "creating secret")
		if _, err := secrets.Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			if k8serrors.IsForbidden(err) {
				log.WarnContext(ctx, "cannot create secret: forbidden")
				return nil
			}
			log.ErrorContext(ctx, "failed to create secret", "err", err)
			return err
		}
	} else {
		log.InfoContext(ctx, "updating secret")
		if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
			if k8serrors.IsForbidden(err) {
				log.WarnContext(ctx, "cannot create secret: forbidden")
				return nil
			}
			log.ErrorContext(ctx, "failed to update secret", "err", err)
			return err
		}
	}
	return nil
}

func updateCredentials(ctx *cli.Context) error {
	lock.Lock()
	defer lock.Unlock()


	accountID := ctx.String("aws-account-id")
	region := ctx.String("aws-region")
	username := "AWS"
	password, err := getPassword(ctx.Context, region)
	if err != nil {
		return err
	}

	registry := fmt.Sprintf(
		"https://%s.dkr.ecr.%s.amazonaws.com/",
		accountID, region,
	)
	token := base64.StdEncoding.EncodeToString([]byte(
		fmt.Sprintf("%s:%s", username, password),
	))

	data, err := json.Marshal(map[string]map[string]map[string]string{
		"auths": {
			registry: {
				"auth": token,
			},
		},
	})
	if err != nil {
		slog.ErrorContext(ctx.Context, "failed to generate docker config", "err", err)
		return err
	}
	credentials = map[string]string{
		".dockerconfigjson": string(data),

	}
	return nil
}

func getPassword(ctx context.Context, region string) (string, error) {
	stdout := bytes.NewBuffer([]byte{})
	stderr := bytes.NewBuffer([]byte{})
	cmd := exec.CommandContext(
		ctx, "/usr/local/bin/aws",
		"ecr", "get-login-password", "--region", region,
	)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Run(); err != nil {
		slog.ErrorContext(
			ctx, "failed to get password", 
			"aws-region", region, 
			"stderr", stderr.String(),
			"err", err,
		)
		return "", err
	}
	return stdout.String(), nil
}

func isMatching(namespace *corev1.Namespace) bool {
	if namespace.Name == "kube-system" {
		return false
	}
	if namespaces := namespaces.Value(); len(namespaces) > 0 {
		if !slices.Contains(namespaces, namespace.Name) {
			return false
		}
	}
	for _, selector := range selectors.Value() {
		keyvalue := strings.SplitN(selector, "=", 2)
		value, ok := namespace.Labels[keyvalue[0]]
		if !ok {
			return false
		}
		if len(keyvalue) == 2 && keyvalue[1] != value {
			return false
		}
	}

	return true
}
