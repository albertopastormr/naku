#!/bin/bash

INPUT_TOPIC_NAME=$1
OUTPUT_TOPIC_NAME=$2
INPUT_SUB_NAME=$3
OUTPUT_SUB_NAME=$4


# create topics
gcloud pubsub topics create $INPUT_TOPIC_NAME
gcloud pubsub topics create $OUTPUT_TOPIC_NAME

# create subscriptions
gcloud pubsub subscriptions create $INPUT_SUB_NAME --topic=$INPUT_TOPIC_NAME
gcloud pubsub subscriptions create $OUTPUT_SUB_NAME --topic=$OUTPUT_TOPIC_NAME
