/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"crypto/tls"
	"flag"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
	"de.yusaozdemir.resource-action-operator/internal/controller"
	"de.yusaozdemir.resource-action-operator/internal/engine"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(opsv1alpha1.AddToScheme(scheme))
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var secureMetrics bool
	var enableHTTP2 bool
	var enableWebhook bool

	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0",
		"The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"Serve metrics securely via HTTPS.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"Enable HTTP/2 for metrics and webhook servers")
	flag.BoolVar(&enableWebhook, "enable-webhook", false,
		"Enable admission webhook registration and serving")

	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "Webhook cert directory")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "Webhook cert name")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "Webhook cert key")

	flag.StringVar(&metricsCertPath, "metrics-cert-path", "", "Metrics cert directory")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "Metrics cert name")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "Metrics cert key")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	var tlsOpts []func(*tls.Config)
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	webhookTLSOpts := tlsOpts
	if webhookCertPath != "" {
		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "failed to init webhook cert watcher")
			os.Exit(1)
		}
		webhookTLSOpts = append(webhookTLSOpts, func(cfg *tls.Config) {
			cfg.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	metricsOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if secureMetrics {
		metricsOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	if metricsCertPath != "" {
		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "failed to init metrics cert watcher")
			os.Exit(1)
		}
		metricsOptions.TLSOpts = append(metricsOptions.TLSOpts, func(cfg *tls.Config) {
			cfg.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "4226e2fa.yusaozdemir.de",
		// Secrets and Pods are only read on demand (header/TLS secrets, job pod
		// details and logs). Read them directly from the API server so the
		// manager does not start cluster-wide informers, which would require
		// list/watch on all Secrets and Pods and keep them in memory.
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{&corev1.Secret{}, &corev1.Pod{}},
			},
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// =========================
	// Event Engine initialisieren
	// =========================
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to create kubernetes clientset")
		os.Exit(1)
	}

	exec := engine.NewK8sExecutor(mgr.GetClient(), clientset, mgr.GetEventRecorderFor("resource-action-operator"))

	eng, err := engine.New(mgr.GetConfig(), exec)
	if err != nil {
		setupLog.Error(err, "unable to create event engine")
		os.Exit(1)
	}

	if err = (&controller.ResourceActionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Engine: eng,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ResourceAction")
		os.Exit(1)
	}
	if enableWebhook {
		if err = (&opsv1alpha1.ResourceAction{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "ResourceAction")
			os.Exit(1)
		}
	}

	if metricsCertWatcher != nil {
		_ = mgr.Add(metricsCertWatcher)
	}
	if webhookCertWatcher != nil {
		_ = mgr.Add(webhookCertWatcher)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
