#!/usr/bin/env bash
set -euo pipefail
V=knative-v1.23.0
kind create cluster --config infra/kind/cluster.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/$V/serving-crds.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/$V/serving-core.yaml
kubectl apply -f https://github.com/knative-extensions/net-kourier/releases/download/$V/kourier.yaml
kubectl patch configmap/config-network --namespace knative-serving --type merge --patch-file infra/knative/patches/ingress-class.json
kubectl wait --for=condition=Available deployment --all -n knative-serving --timeout=300s
kubectl wait --for=condition=Available deployment --all -n kourier-system --timeout=300s
kubectl patch service kourier -n kourier-system --type merge --patch-file infra/knative/patches/nodeport.json
kubectl patch configmap/config-domain -n knative-serving --type merge --patch-file infra/knative/patches/domain.json
kubectl apply -f infra/knative/namespace.yaml
kubectl apply -f infra/knative/periodic.yaml -f infra/knative/steady.yaml -f infra/knative/sparse.yaml
kubectl wait --for=condition=Ready ksvc --all -n csc3104 --timeout=300s
kubectl get ksvc -n csc3104