# Aviation Delay — Big Data Lakehouse for Weather-Driven Delay Prediction

Short description
-----------------
This repository implements a Big Data Lakehouse system that integrates flight operational data, aircraft information, and weather data to predict the probability of flight delays caused by weather conditions. It contains Jupyter notebooks, Python code, and a Docker setup for packaging the service.

Table of contents
-----------------
- [Prerequisites](#prerequisites)
- [Clone](#clone)
- [Build Docker image](#build-docker-image)
- [Push Docker image (Docker Hub)](#push-docker-image-docker-hub)
- [Push Docker image (Google Artifact Registry / GCR)](#push-docker-image-google-artifact-registry--gcr)
- [Deploy to Google Cloud (Cloud Run)](#deploy-to-google-cloud-cloud-run)
- [Environment variables & secrets](#environment-variables--secrets)

Prerequisites
-------------
- Git
- Docker (engine + CLI)
- (For GCP) Google Cloud SDK (gcloud) configured and authenticated
- Optional: Docker Hub account or container registry account

Clone
-----
```bash
git clone https://github.com/jonhrh1348/aviation-delay.git
cd aviation-delay
```

Build Docker image
------------------
From the repo root (assumes a Dockerfile exists):
```bash
docker build -t aviation-delay:latest .
```

Push Docker image (Docker Hub)
------------------------------
1. Log in to Docker Hub:
```bash
docker login
```
2. Tag & push (replace DOCKERHUB_USER):
```bash
docker tag aviation-delay:latest DOCKERHUB_USER/aviation-delay:latest
docker push DOCKERHUB_USER/aviation-delay:latest
```

Push Docker image (Google Artifact Registry / GCR)
--------------------------------------------------
Option A — Cloud Build + Cloud Run (simple):
```bash
gcloud builds submit --tag gcr.io/<PROJECT_ID>/aviation-delay:latest
```
Option B — Artifact Registry (recommended):
```bash
# Enable and create repo if needed (one-time)
gcloud services enable artifactregistry.googleapis.com
gcloud artifacts repositories create aviation-repo \
  --repository-format=docker --location=us-central1

# Configure Docker auth & push
gcloud auth configure-docker us-central1-docker.pkg.dev

docker tag aviation-delay:latest \
  us-central1-docker.pkg.dev/<PROJECT_ID>/aviation-repo/aviation-delay:latest

docker push us-central1-docker.pkg.dev/<PROJECT_ID>/aviation-repo/aviation-delay:latest
```

Deploy to Google Cloud (Cloud Run)
----------------------------------
Cloud Run is quickest for containerized web services (managed).
Option 1 — Deploy image you pushed with Cloud Build:
```bash
gcloud run deploy aviation-delay \
  --image gcr.io/<PROJECT_ID>/aviation-delay:latest \
  --platform managed --region us-central1 --allow-unauthenticated
```
Option 2 — Deploy directly from local source (Cloud Build triggered automatically):
```bash
gcloud run deploy aviation-delay --source . --region us-central1 --platform managed
```
For GKE, create a Kubernetes deployment referencing the image in GCR/Artifact Registry and expose via a Service/Ingress.

Environment variables & secrets
-------------------------------
- Do NOT bake secrets in images. Use:
  - GCP: Secret Manager or set runtime env vars in Cloud Run with `--set-secrets` or `--set-env-vars`
- Example Cloud Run env:
```bash
gcloud run deploy aviation-delay \
  --image gcr.io/<PROJECT_ID>/aviation-delay:latest \
  --set-env-vars DATA_BUCKET=gs://my-bucket,MODEL_PATH=/models/latest
```
