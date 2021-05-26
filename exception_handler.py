from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import AlreadyExists, InvalidArgument, PermissionDenied, NotFound
import sys

def exception_handler(func):

    def _handle_errors(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AlreadyExists:
            print(f"WARNING: Resource '{args[2]}' already exists")
        except InvalidArgument:
            print(f"WARNING: Invalid argument used or resource '{args[2]}' already exists")
        except NotFound:
            print(f"WARNING: Resource '{args[2] if len(args) > 2 else args[1]}' not found")
        except DefaultCredentialsError:
            print("ERROR: Could not automatically determine credentials; GOOGLE_APPLICATION_CREDENTIALS is not declare in your " 
                    "environment or the path to which it points doesn't exist."
                    " If you don't have a GCP credentials file yet, launch 'python naku.py auth -p <PROJECT_ID>' and follow the steps.")
            sys.exit(1)
        except PermissionDenied:
            print('ERROR: You have to enable the PubSub API: https://console.developers.google.com/apis/api/pubsub.googleapis.com/'
            ' and the Dataproc API: https://console.cloud.google.com/apis/library/dataproc.googleapis.com'
            f' for the project {args[1]} or execute "python naku.py auth -p <PROJECT_ID>"')
            sys.exit(1)
            
    return _handle_errors

