import os


def auth_gcp(service_account, project_id, filename):
    os.system('gcloud auth login')
    os.system('gcloud init')
    os.system(f'gcloud iam service-accounts create {service_account}')
    os.system(f'gcloud projects add-iam-policy-binding {project_id} --member="serviceAccount:{service_account}@{project_id}.iam.gserviceaccount.com" --role="roles/owner"')
    os.system(f'gcloud iam service-accounts keys create {filename}.json --iam-account={service_account}@{project_id}.iam.gserviceaccount.com')
    print('Execute the following command or save the env variable in your corresponding shell profile'
            f': export GOOGLE_APPLICATION_CREDENTIALS="{os.getcwd()}/{filename}.json"')
    print('You have to enable the PubSub API: https://console.developers.google.com/apis/api/pubsub.googleapis.com/'
            ' and the Dataproc API: https://console.cloud.google.com/apis/library/dataproc.googleapis.com'
            f' for the project {project_id}')
    