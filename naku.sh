#!/bin/bash

SERVICE_ACCOUNT_NAME="naku-dataproc-service-account"
PROJECT=$(gcloud config get-value project)
INPUT_TOPIC_NAME="naku-input-topic"
OUTPUT_TOPIC_NAME="naku-output-topic"
INPUT_SUB_NAME="naku-input-sub"
OUTPUT_SUB_NAME="naku-output-sub"
CLUSTER_NAME="naku-dataproc-cluster"
CLUSTER_REGION="europe-west1"
CLUSTER_ZONE="europe-west1-b"

./auth.sh

./init.sh $PROJECT $SERVICE_ACCOUNT_NAME $INPUT_TOPIC_NAME $OUTPUT_TOPIC_NAME \
            $INPUT_SUB_NAME $OUTPUT_SUB_NAME $CLUSTER_NAME $CLUSTER_REGION $CLUSTER_ZONE

./delete.sh $PROJECT $SERVICE_ACCOUNT_NAME $INPUT_TOPIC_NAME $OUTPUT_TOPIC_NAME \
            $INPUT_SUB_NAME $OUTPUT_SUB_NAME $CLUSTER_NAME