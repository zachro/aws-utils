from os import path

import boto3
from botocore.response import StreamingBody


class AmazonS3:

    def __init__(self, client, resource):
        self.client = client
        self.resource = resource

    def list_objects(self, bucket, prefix=None, include_metadata=False) -> list:
        """
        Lists all objects in the specified bucket.
        :param bucket: The name of the bucket
        :param prefix: Optional - list obly objects matching a given prefix
        :param include_metadata: Optional - if true, return objects as dicts containing all S3 metadata
        :return: The list of objects
        """
        objects = []
        kwargs = {'Bucket': bucket}
        if prefix:
            kwargs['Prefix'] = prefix

        is_truncated = True
        while is_truncated:
            response = self.client.list_objects_v2(**kwargs)
            if not response.get('Contents'):
                return objects

            for item in response['Contents']:
                if include_metadata:
                    objects.append(item)
                else:
                    objects.append(item['Key'])

            is_truncated = response['IsTruncated']
            kwargs['ContinuationToken'] = response.get('NextContinuationToken')

        return objects

    def put_object(self, bucket, key, local_file_path=None, file_bytes=None) -> None:
        """
        Puts a local object to S3. Either the file bytes or a path to the local file must be provided.
        :param bucket: The name of the bucket
        :param key: The S3 key to upload to
        :param local_file_path: The path to the local file
        :param file_bytes: The bytes to upload
        """
        def upload_to_s3(byte_array):
            self.resource.Object(bucket, key).put(Body=byte_array)

        if file_bytes:
            upload_to_s3(file_bytes)
        else:
            with open(local_file_path, 'rb') as local_file:
                self.resource.Object(bucket, key).put(Body=local_file)

    def copy_object(self, source_bucket, source_key, dest_bucket, dest_key) -> None:
        """
        Copies an object from one S3 location to another.
        :param source_bucket: The name of the source bucket
        :param source_key: The source key
        :param dest_bucket: The name of the destination bucket
        :param dest_key: The destination key
        """
        self.resource.Object(dest_bucket, dest_key).copy_from(CopySource=path.join(source_bucket, source_key))

    def stream_object(self, bucket, key) -> StreamingBody:
        """
        Streams an object from S3.
        :param bucket: The name of the bucket
        :param key: The S3 key
        :return: A StreamingBody for the S3 object
        """
        return self.resource.Object(bucket, key).get()['Body']

    def download_object(self, bucket, key, dest_path) -> None:
        """
        Downloads an object from S3 to a local directory.
        :param bucket: The name of the bucket
        :param key: The key for the object to download
        :param dest_path: The local path for the downloaded file, including the name of the file
        """
        self.resource.Bucket(bucket).download_file(key, dest_path)

    @staticmethod
    def with_profile(profile):
        session = boto3.Session(profile_name=profile)
        return AmazonS3(session.client('s3'), session.resource('s3'))

    @staticmethod
    def default_client():
        return AmazonS3(boto3.client('s3'), boto3.resource('s3'))
