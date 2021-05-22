import argparse

from pubsub import PubSubClient
from dataproc import DataprocClient
from auth import auth_gcp


# once I create config.yaml, project_id could be set there
# and the required=True could evaluate whether project_id has been
# set in the config.yaml
# The import of the variables from this file should also be controlled
# with exceptions
SERVICE_ACCOUNT_NAME="naku-serv-acc"
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
            
    subparsers = parser.add_subparsers(title='action', 
                                        dest='action',
                                        description='Valid subcommands',
                                        help='action to perform on the GCP infrastructure')

    parser_init = subparsers.add_parser('init')

    parser_init.add_argument("-p", '--project_id', dest='project_id', type=str, required=True,
                           help="your Google Cloud project ID")

    parser_init.add_argument('--region', 
                            choices=['asia-east1', 'asia-northeast1', 'asia-southeast1',
                            'europe-west1','us-central1','us-east1','us-west1'],
                            help='Location where the resources should be allocated')
    
    parser_auth = subparsers.add_parser('auth')

    parser_auth.add_argument("-p", '--project_id', dest='project_id', type=str, required=True,
                           help="your Google Cloud project ID")
    
    parser_auth.add_argument("-f", '--filename', dest='filename', type=str, default=SERVICE_ACCOUNT_NAME,
                           help="path to store the service account credentials file")

    parser_launch = subparsers.add_parser('launch')

    parser_launch.add_argument("-p", '--project_id', dest='project_id', type=str, required=True,
                           help="your Google Cloud project ID")

    parser_delete = subparsers.add_parser('delete')

    parser_delete.add_argument("-p", '--project_id', dest='project_id', type=str, required=True,
                           help="your Google Cloud project ID")

    parser_list = subparsers.add_parser('list')

    parser_list.add_argument("-p", '--project_id', dest='project_id', type=str, required=True,
                           help="your Google Cloud project ID")

    args = parser.parse_args()
    
    return args

if __name__ == '__main__':
    args = parse_args()
    print(args)

    if args.action == "init":
        ps = PubSubClient()
        dp = DataprocClient()
        ps.create_topic(args.project_id, INPUT_TOPIC_NAME)
        ps.create_topic(args.project_id, OUTPUT_TOPIC_NAME)
        ps.list_topics(args.project_id)
    elif args.action == "auth":
        auth_gcp(SERVICE_ACCOUNT_NAME, args.project_id, args.filename)
    elif args.action == "launch":
        pass
        # publish_messages(args.project_id, args.topic_id)
    elif args.action == "delete":
        ps = PubSubClient()
        dp = DataprocClient()
        ps.delete_topic(args.project_id, INPUT_TOPIC_NAME)
        ps.delete_topic(args.project_id, OUTPUT_TOPIC_NAME)
    elif args.action == "list":
        ps = PubSubClient()
        dp = DataprocClient()
        ps.list_topics(args.project_id)
        ps.list_subscriptions(args.project_id)