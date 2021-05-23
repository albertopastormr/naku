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
    def submit_job(self, project_id, cluster_name):

        job = {
                'placement': {
                    'cluster_name': cluster_name
                },
                'pyspark_job': {
                    'main_python_file_uri': 'model.py'
                }
            }

        operation = self.job_client.submit_job(
            request={"project_id": project_id, "region": self.region, "job": job}
        )

        job_id = operation.reference.job_id
        print(f"Job submitted: {job_id}")

    @exception_handler
    def list_jobs(self, project_id):
        print(f"Listing existing Dataproc jobs in project {project_id}")
        for job in self.job_client.list_jobs(request={"project_id": project_id, "region":self.region}):
            print("  --> "  + f"job id: {job.reference.job_id} [{job.pyspark_job.main_python_file_uri}]"
                            + f" executed on cluster '{job.placement.cluster_name}'"
                            + f"\n\tStatus: {job.status.state.name}: {job.status.details}"
                            + f' ({job.status.state_start_time.strftime("%H:%M:%S %d-%m-%Y")})')