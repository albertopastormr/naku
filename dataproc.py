from google.cloud import dataproc_v1 as dataproc
from exception_handler import exception_handler


class DataprocClient:

    @exception_handler
    def __init__(self, region):
        self.region = region
        self.cluster = dataproc.ClusterControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
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

        self.cluster.create_cluster(
            request={"project_id": project_id, "region": self.region, "cluster": cluster_config}
        )
        print(f"Cluster created: {cluster_name}")

    @exception_handler
    def delete_cluster(self, project_id, cluster_name):

        self.cluster.delete_cluster(
            request={"project_id": project_id, "region": self.region, "cluster_name": cluster_name}
        )
        
        print(f"Cluster deleted: {cluster_name}")

    @exception_handler
    def list_clusters(self, project_id):
        print(f"Listing existing clusters in project {project_id}")
        for cluster in self.cluster.list_clusters(request={"project_id": project_id, "region":self.region}):
            print("  --> " + cluster.cluster_name + f"\n\tnumber of masters: {cluster.config.master_config.num_instances}"
                            + f"\n\tnumber of workers: {cluster.config.worker_config.num_instances}")