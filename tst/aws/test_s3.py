from unittest import TestCase
from unittest.mock import Mock, patch, mock_open

from aws.s3 import AmazonS3

BUCKET_1 = 'not-a-real-bucket'
KEY_1 = 'path/to/folder/'
BUCKET_2 = 'also-not-a-real-bucket'
KEY_2 = 'path/to/other/folder/'

CLIENT_LIST_OBJECTS_RESPONSE_TRUNCATED = {
    'Contents': [
        {'Key': 'file1.txt', 'OtherMetadata': 'metadata'},
        {'Key': 'file2.txt', 'OtherMetadata': 'metadata'}
    ],
    'IsTruncated': True
}
CLIENT_LIST_OBJECTS_RESPONSE_NOT_TRUNCATED = {
    'Contents': [
        {'Key': 'file3.txt', 'OtherMetadata': 'metadata'},
        {'Key': 'file4.txt', 'OtherMetadata': 'metadata'}
    ],
    'IsTruncated': False
}


class AmazonS3Test(TestCase):

    @patch('boto3.client')
    def test__list_objects__no_metadata(self, mock_client):
        mock_client.list_objects_v2 = Mock(return_value=CLIENT_LIST_OBJECTS_RESPONSE_NOT_TRUNCATED)
        expected = ['file3.txt', 'file4.txt']

        actual = AmazonS3(mock_client, None).list_objects(BUCKET_1, prefix=KEY_1)

        self.assertEqual(expected, actual)

    @patch('boto3.client')
    def test__list_objects__with_metadata(self, mock_client):
        mock_client.list_objects_v2 = Mock(return_value=CLIENT_LIST_OBJECTS_RESPONSE_NOT_TRUNCATED)
        expected = [
            {'Key': 'file3.txt', 'OtherMetadata': 'metadata'},
            {'Key': 'file4.txt', 'OtherMetadata': 'metadata'}
        ]

        actual = AmazonS3(mock_client, None).list_objects(BUCKET_1, prefix=KEY_1, include_metadata=True)

        self.assertEqual(expected, actual)

    @patch('boto3.client')
    def test__list_objects__response_truncated(self, mock_client):
        mock_client.list_objects_v2 = Mock(side_effect=[CLIENT_LIST_OBJECTS_RESPONSE_TRUNCATED,
                                                        CLIENT_LIST_OBJECTS_RESPONSE_NOT_TRUNCATED])
        expected = ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt']

        actual = AmazonS3(mock_client, None).list_objects(BUCKET_1, prefix=KEY_1)

        self.assertEqual(expected, actual)

    @patch('boto3.resource')
    def test__put_object__file_bytes(self, mock_resource):
        mock_object = Mock()
        mock_object.put = Mock()
        mock_resource.Object = Mock(return_value=mock_object)

        AmazonS3(None, mock_resource).put_object(BUCKET_1, KEY_1, file_bytes='bytes')

        mock_object.put.assert_called_with(Body='bytes')

    @patch('boto3.resource')
    def test__put_object__file_path(self, mock_resource):
        mock_object = Mock()
        mock_object.put = Mock()
        mock_resource.Object = Mock(return_value=mock_object)
        file_content = 'file content'

        with patch('builtins.open', mock_open(read_data=file_content)):
            AmazonS3(None, mock_resource).put_object(BUCKET_1, KEY_1, local_file_path='test/file/path')

        mock_object.put.assert_called_once()

    @patch('boto3.resource')
    def test__copy_object(self, mock_resource):
        mock_object = Mock()
        mock_object.copy_from = Mock()
        mock_resource.Object = Mock(return_value=mock_object)

        AmazonS3(None, mock_resource).copy_object(BUCKET_1, KEY_1, BUCKET_2, KEY_2)

        mock_object.copy_from.assert_called_with(CopySource='not-a-real-bucket/path/to/folder/')

    @patch('boto3.resource')
    def test__stream_object(self, mock_resource):
        mock_streaming_body = Mock()
        mock_object = Mock()
        mock_object.get = Mock(return_value={'Body': mock_streaming_body})
        mock_resource.Object = Mock(return_value=mock_object)

        actual = AmazonS3(None, mock_resource).stream_object(BUCKET_1, KEY_1)

        self.assertEqual(mock_streaming_body, actual)

    @patch('boto3.resource')
    def test__download_object(self, mock_resource):
        mock_bucket = Mock()
        mock_bucket.download_file = Mock()
        mock_resource.Bucket = Mock(return_value=mock_bucket)

        AmazonS3(None, mock_resource).download_object(BUCKET_1, KEY_1, 'local/file/path/')

        mock_bucket.download_file.assert_called_with(KEY_1, 'local/file/path/')

    @patch('boto3.Session')
    def test__with_profile(self, mock_session):
        AmazonS3.with_profile('not-a-real-profile')

        mock_session.assert_called_with(profile_name='not-a-real-profile')
