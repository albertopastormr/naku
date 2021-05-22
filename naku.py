import argparse

import os
import sys


SERVICE_ACCOUNT_NAME="naku-dataproc-service-account"
INPUT_TOPIC_NAME="naku-input-topic"
OUTPUT_TOPIC_NAME="naku-output-topic"
INPUT_SUB_NAME="naku-input-sub"
OUTPUT_SUB_NAME="naku-output-sub"
CLUSTER_NAME="naku-dataproc-cluster"
CLUSTER_REGION="europe-west1"
CLUSTER_ZONE="europe-west1-b"

def parse_args():
    parser = argparse.ArgumentParser(prog='naku',
            description='Test ML models over Google Cloud Platform')
    parser.add_argument("-p", '--project_id', type=str, required=True,
                           help="set project id on GCP")
            
    subparsers = parser.add_subparsers(title='subcommands', 
                                        dest='subcommand',
                                        description='Valid subcommands',
                                        help='action to perform on the GCP infrastructure')

    parser_init = subparsers.add_parser('init')

    parser_init.add_argument('--region', 
                            choices=['asia-east1', 'asia-northeast1', 'asia-southeast1',
                            'europe-west1','us-central1','us-east1','us-west1'],
                            help='Location where the resources should be allocated')
    
    parser_auth = subparsers.add_parser('auth')

    parser_launch = subparsers.add_parser('launch')

    parser_delete = subparsers.add_parser('delete')

    args = parser.parse_args()
    
    return vars(args)

if __name__ == '__main__':
    args = parse_args()
    print(args)