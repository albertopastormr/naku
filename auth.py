import os


def auth_gcp(service_account, project_id, filename):
    os.system('gcloud auth login')
    os.system('gcloud init')
    os.system(f'gcloud iam service-accounts create {service_account}')
    print(f'Service account created: {service_account}')
    os.system(f'gcloud projects add-iam-policy-binding {project_id} --member="serviceAccount:{service_account}@{project_id}.iam.gserviceaccount.com" --role="roles/owner"')
    os.system(f'gcloud iam service-accounts keys create {filename}.json --iam-account={service_account}@{project_id}.iam.gserviceaccount.com')
    print(f'Credentials stored: {os.getcwd()}/{filename}.json')
    print('Execute the following command or save the env variable in your corresponding shell profile'
            f': export GOOGLE_APPLICATION_CREDENTIALS="{os.getcwd()}/{filename}.json"')
    os.system("gcloud services enable dataproc.googleapis.com pubsub.googleapis.com ")
    print("Enabled PubSub and Dataproc APIs")
    
def delete_service_account(service_account, project_id):
    os.system(f' gcloud iam service-accounts delete {service_account}@{project_id}.iam.gserviceaccount.com --quiet')
   