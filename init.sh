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

gcloud services enable \
    dataproc.googleapis.com \
    pubsub.googleapis.com 

./init_pubsub.sh $INPUT_TOPIC_NAME $OUTPUT_TOPIC_NAME $INPUT_SUB_NAME $OUTPUT_SUB_NAME

./init_dataproc.sh $INPUT_SUB_NAME $OUTPUT_TOPIC_NAME $CLUSTER_NAME $CLUSTER_REGION $CLUSTER_ZONE