#!/bin/bash

SERVICE_ACCOUNT_NAME="naku-dataproc-service-account"
PROJECT=$(gcloud info --format='value(config.project)')
INPUT_SUB_NAME=$1
OUTPUT_TOPIC_NAME=$2
CLUSTER_NAME=$3 # which is going to be hardcoded as "naku-cluster" in the main sh
CLUSTER_REGION=$4
CLUSTER_ZONE=$5


gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME

# enable dataproc permissions to create clusters, execute jobs and read-write from pub/sub
gcloud projects add-iam-policy-binding $PROJECT \
    --role roles/dataproc.worker \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"

gcloud beta pubsub subscriptions add-iam-policy-binding \
    $INPUT_SUB_NAME \
    --role roles/pubsub.subscriber \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"

gcloud beta pubsub topics add-iam-policy-binding \
    $OUTPUT_TOPIC_NAME \
    --role roles/pubsub.publisher \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"

# create dataproc cluster
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$CLUSTER_REGION \
    --zone=$CLUSTER_ZONE \
    --scopes=pubsub \
    --image-version=1.2 \
    --service-account="$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"