import boto3
from botocore.exceptions import ClientError


class S3Connector:
    def configure(self):
        try:
            self.s3_client = boto3.client(
                service_name="s3", region_name="ap-southeast-2",
            )
            self.s3_resource = boto3.resource(
                service_name="s3", region_name="ap-southeast-2",
            )
        except Exception as e:
            print(e)
            raise

    def list_bucket(self):
        # Call S3 to list current buckets
        response = self.s3_client.list_buckets()
        # Get a list of all bucket names from the response
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        # Print out the bucket list
        print("Bucket List: %s" % buckets)
        return buckets

    def create_bucket(self, bucket_name):
        self.s3_client.create_bucket(name=bucket_name)

    def upload_file(
        self, file_name, bucket=None, object_name=None,
    ):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        if bucket is None:
            bucket = "ac-shopping-datalake"

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        try:
            self.s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            self.parent_task.logging.log_error(f"S3 File Upload Failed: {e}")
            return False
        return True

    def clean_up_s3_folder(self, bucket, folder_path):
        s3 = self.s3_resource
        bucket = s3.Bucket(bucket)
        for obj in bucket.objects.filter(Prefix=folder_path):
            s3.Object(bucket.name, obj.key).delete()
