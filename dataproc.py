import os
from google.cloud import dataproc_v1 as dataproc

#from google.cloud import storage

from exception_handler import exception_handler
import re
from datetime import datetime


class DataprocClient:

    @exception_handler
    def __init__(self, region):
        self.region = region
        self.api_endpoint = f"{region}-dataproc.googleapis.com:443"
        self.cluster_client = dataproc.ClusterControllerClient(
            client_options={"api_endpoint": self.api_endpoint}
        )
        self.job_client = dataproc.JobControllerClient(
            client_options={'api_endpoint': self.api_endpoint}
        )


    @exception_handler
    def create_cluster(self, project_id, cluster_name):
        cluster_config = {
            "project_id": project_id,
            "cluster_name": cluster_name,
            "config": { 
                "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
                "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
                "initialization_actions":[ {"executable_file":f'gs://goog-dataproc-initialization-actions-{self.region}/python/pip-install.sh'}],
                "gce_cluster_config": { # TODO: take pip packages from requirements.txt
                    "metadata": {'PIP_PACKAGES':'apache-beam[gcp]==2.29.0'},
                    "service_account_scopes": [
                        "https://www.googleapis.com/auth/cloud-platform",
                        "https://www.googleapis.com/auth/pubsub"
                    ]
                }
            },
        }

        self.cluster_client.create_cluster(
            request={"project_id": project_id, "region": self.region, "cluster": cluster_config}
        )
        print(f"Cluster created: {cluster_name}")

    @exception_handler
    def delete_cluster(self, project_id, cluster_name):

        self.cluster_client.delete_cluster(
            request={"project_id": project_id, "region": self.region, "cluster_name": cluster_name}
        )
        
        print(f"Cluster deleted: {cluster_name}")

    @exception_handler
    def list_clusters(self, project_id):
        print(f"Listing existing clusters in project {project_id}")
        for cluster in self.cluster_client.list_clusters(request={"project_id": project_id, "region":self.region}):
            print("  --> " + cluster.cluster_name + f" {cluster.status.state.name} " 
                            + f"\n\tNumber of masters: {cluster.config.master_config.num_instances}"
                            + f"\n\tNumber of workers: {cluster.config.worker_config.num_instances}")

    @exception_handler
    def submit_job(self, project_id, cluster_name, bucket_name, pyspark_filename, input_sub, output_topic,):

        job = {
                'placement': {
                    'cluster_name': cluster_name
                },
                'pyspark_job': {
                    'main_python_file_uri': f'gs://{bucket_name}/{pyspark_filename}',
                    'args': [f'--project_id={project_id}', f'--input_sub={input_sub}',
                             f'--output_topic={output_topic}']
                }
            }

        operation = self.job_client.submit_job(
            request={"project_id": project_id, "region": self.region, "job": job}
        )

        job_id = operation.reference.job_id
        print(f"Job submitted: {job_id}")
        print(f"You can check its execution details and output at:"
                f" https://console.cloud.google.com/dataproc/jobs/{job_id}?region={self.region}&project={project_id}")

    @exception_handler
    def list_jobs(self, project_id):
        print(f"Listing existing Dataproc jobs in project {project_id}")
        for job in self.job_client.list_jobs(request={"project_id": project_id, "region":self.region}):
            print("  --> "  + f"job id: {job.reference.job_id} [{job.pyspark_job.main_python_file_uri}]"
                            + f" executed on cluster '{job.placement.cluster_name}'"
                            + f"\n\tStatus: {job.status.state.name}: {job.status.details}"
                            + f' ({job.status.state_start_time.strftime("%H:%M:%S %d-%m-%Y")})'
                            + f' [https://console.cloud.google.com/dataproc/jobs/{job.reference.job_id}?region={self.region}&project={project_id}]')

    def delete_jobs(self, project_id):
        for job in self.job_client.list_jobs(request={"project_id": project_id, "region":self.region}):
            self.job_client.delete_job(
                request={"project_id":project_id, "region":self.region, "job_id":job.reference.job_id}
            )
            print(f"Job deleted: {job.reference.job_id}")

    # delete jobs https://googleapis.dev/python/dataproc/latest/dataproc_v1beta2/job_controller.html#google.cloud.dataproc_v1beta2.services.job_controller.JobControllerAsyncClient.delete_job


if __name__ == '__main__':
    dp = DataprocClient("europe-west1")
    #dp.delete_cluster("naku-demo", "naku-dataproc-cluster")
    dp.create_cluster("naku-demo", "naku-dataproc-cluster")



""" # this corresponds to the cluster creation command via CLI
gcloud dataproc clusters create naku-dataproc-cluster \
    --region europe-west1 \
    --metadata 'PIP_PACKAGES=apache-beam==2.29.0' \
    --initialization-actions gs://goog-dataproc-initialization-actions-europe-west2/python/pip-install.sh
docs: https://www.linkedin.com/pulse/how-do-you-create-gcp-data-proc-cluster-passing-json-rajib-deb/?trk=related_artice_How%20do%20you%20create%20a%20GCP%20data%20proc%20cluster%20by%20passing%20a%20JSON%3F_article-card_title
docs: https://cloud.google.com/dataproc/docs/reference/rpc/google.cloud.dataproc.v1#google.cloud.dataproc.v1.ClusterConfig
docs: https://cloud.google.com/dataproc/docs/tutorials/python-configuration
"""