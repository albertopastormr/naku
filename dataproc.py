from google.cloud import dataproc_v1 as dataproc

class DataprocClient:

    def __init__(self):
        self.cluster_client = dataproc.ClusterControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )