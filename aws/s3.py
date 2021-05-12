import boto3


class AmazonS3:

    def __init__(self, client, resource):
        self.client = client
        self.resource = resource

    def list_objects(self, bucket, prefix=None):
        pass

    def copy_object(self, source_bucket, source_key, dest_bucket, dest_key):
        pass

    def stream_object(self, bucket, key):
        pass

    def download_object(self, bucket, key, dest_path):
        pass

    @staticmethod
    def with_profile(profile):
        pass

    @staticmethod
    def default_client():
        pass
