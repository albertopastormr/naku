from google.cloud import storage

from exception_handler import exception_handler

class StorageClient:

    @exception_handler
    def __init__(self):
        self.storage_client = storage.Client()

    @exception_handler
    def create_bucket(self, bucket_name, location='eu', storage_class="COLDLINE"):
        """Create a new bucket in specific location with storage class"""

        bucket = self.storage_client.bucket(bucket_name)
        bucket.storage_class = storage_class
        new_bucket = self.storage_client.create_bucket(bucket, location=location)

        print(
            "Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
            )
        )

    @exception_handler
    def list_buckets(self):
        """Lists all buckets."""

        buckets = self.storage_client.list_buckets()
        print(f"Listing existing storage buckets")
        for bucket in buckets:
            print("  --> "  + bucket.name)

    @exception_handler
    def upload_file(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    
    @exception_handler
    def delete_buckets_by_prefix(self, prefix):
        """Deletes all the buckets corresponding. Every bucket must be empty."""

        #bucket = self.storage_client.get_bucket(bucket_name)
        #bucket.delete()

        buckets = self.storage_client.list_buckets()
        
        for bucket in buckets:
            if bucket.name.startswith(prefix):
                bucket.delete(force=True)
                print(f"Bucket {bucket.name} deleted")

    @exception_handler
    def delete_bucket(self, bucket_name):
        """Deletes a bucket"""

        bucket = self.storage_client.get_bucket(bucket_name)
        bucket.delete(force=True)

        print(f"Bucket {bucket.name} deleted")

    @exception_handler
    def delete_blob(self, bucket_name, blob_name):
        """Deletes a blob from the bucket."""

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        print(f"Blob {blob_name} deleted.")


if __name__ == "__main__":
    StorageClient().create_bucket("naku-support-bucket")