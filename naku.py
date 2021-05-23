import argparse

from pubsub import PubSubClient
from dataproc import DataprocClient
from auth import auth_gcp, delete_service_account


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
REGION="europe-west1"
ZONE="europe-west1-b"

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

    parser_delete.add_argument('--delete_service_account', dest='del_ser_acc', action='store_true',
                           help="whether to delete the service account related to the infrastructure")

    parser_list = subparsers.add_parser('list')

    parser_list.add_argument("-p", '--project_id', dest='project_id', type=str, required=True,
                           help="your Google Cloud project ID")

    parser_list.add_argument('--only_jobs', dest='only_jobs', action='store_true',
                           help="shows only the jobs previously submitted")

    args = parser.parse_args()
    
    return args

if __name__ == '__main__':
    args = parse_args()
    print(args) # to debug

    if args.action == "init":
        ps = PubSubClient()
        dp = DataprocClient(REGION)
        ps.create_topic(args.project_id, INPUT_TOPIC_NAME)
        ps.create_topic(args.project_id, OUTPUT_TOPIC_NAME)
        ps.create_subscription(args.project_id, INPUT_TOPIC_NAME, INPUT_SUB_NAME)
        ps.create_subscription(args.project_id, OUTPUT_TOPIC_NAME, OUTPUT_SUB_NAME)
        dp.create_cluster(args.project_id, CLUSTER_NAME)
    elif args.action == "auth":
        auth_gcp(SERVICE_ACCOUNT_NAME, args.project_id, args.filename)
    elif args.action == "launch":
        dp = DataprocClient(REGION)
        dp.submit_job(args.project_id, CLUSTER_NAME)
        # publish_messages(args.project_id, args.topic_id)
    elif args.action == "delete":
        ps = PubSubClient()
        dp = DataprocClient(REGION)
        ps.delete_topic(args.project_id, INPUT_TOPIC_NAME)
        ps.delete_topic(args.project_id, OUTPUT_TOPIC_NAME)
        ps.delete_subscription(args.project_id, INPUT_SUB_NAME)
        ps.delete_subscription(args.project_id, OUTPUT_SUB_NAME)
        dp.delete_cluster(args.project_id, CLUSTER_NAME)
        if args.del_ser_acc:
            delete_service_account(SERVICE_ACCOUNT_NAME, args.project_id)
    elif args.action == "list":
        ps = PubSubClient()
        dp = DataprocClient(REGION)
        if not args.only_jobs:
            ps.list_topics(args.project_id)
            ps.list_subscriptions(args.project_id)
            dp.list_clusters(args.project_id)
        dp.list_jobs(args.project_id)