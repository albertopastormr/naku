#!/bin/bash

SERVICE_ACCOUNT_NAME="naku-dataproc-service-account"
PROJECT=$(gcloud info --format='value(config.project)')
INPUT_TOPIC_NAME=$1
OUTPUT_TOPIC_NAME=$2
INPUT_SUB_NAME=$3
OUTPUT_SUB_NAME=$4
CLUSTER_NAME=$5

gcloud dataproc clusters delete $CLUSTER_NAME --quiet
gcloud pubsub topics delete $INPUT_TOPIC_NAME --quiet
gcloud pubsub topics delete $OUTPUT_TOPIC_NAME --quiet
gcloud pubsub subscriptions delete $INPUT_SUB_NAME --quiet
gcloud pubsub subscriptions delete $OUTPUT_SUB_NAME --quiet
gcloud iam service-accounts delete $SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com --quiet