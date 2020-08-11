import unittest

from src.util.move_s3_objects import validate_copy_count, validate_copy, move_object_with_key
from botocore.exceptions import ClientError
from mock import patch
import botocore

orig = botocore.client.BaseClient._make_api_call

class TestMoveS3Objects(unittest.TestCase):

    def test_move_object_with_key_successfully(self):
        response = {'ResponseMetadata': {'RequestId': '5E7F058C0F19D489',
                                         'HTTPStatusCode': 200, 'RetryAttempts': 0},
                    'AcceptRanges': 'bytes', 'ContentLength': 280, 'ETag': '"09c7b314e3acd679d29f21bf57f595f5"',
                    'ContentType': 'application/x-gzip', 'Metadata': {}}

        with patch('boto3.resource.BaseClient._make_api_call', return_value=response):
            try:
                move_object_with_key('source_bucket', 'source_key', 'destination_bucket', 'destination_key')
            except ClientError:
                self.fail("Should not fail")

    def test_validate_copy_successful(self):
        response = {'ResponseMetadata': {'RequestId': '5E7F058C0F19D489',
                                         'HTTPStatusCode': 200, 'RetryAttempts': 0},
                    'AcceptRanges': 'bytes', 'ContentLength': 280, 'ETag': '"09c7b314e3acd679d29f21bf57f595f5"',
                    'ContentType': 'application/x-gzip', 'Metadata': {}}

        with patch('botocore.client.BaseClient._make_api_call', return_value=response):
            try:
                validate_copy('destination_bucket', 'copied_object_key_exists')
            except ClientError:
                self.fail("Should not fail")

    def test_validate_copy_unsuccessful(self):
        parsed_response = {'Error': {'Code': '404', 'Message': 'An error occurred (404) when calling the HeadObject '
                                                               'operation: Not Found'}}

        with patch('botocore.client.BaseClient._make_api_call', side_effect=ClientError(parsed_response, "HeadObject")):
            with self.assertRaises(ClientError):
                validate_copy('destination_bucket', 'copied_object_key_does_not_exist')

    def test_validate_copy_count_successful(self):
        response = {'ResponseMetadata': {'RequestId': '0DA5ACACE26C14D9',
                                         'HTTPStatusCode': 200, 'RetryAttempts': 0},
                    'IsTruncated': False, 'Contents': [{'Key': 'source_prefix/pa_data_output_011.csv'},
                                                       {'Key': 'source_prefix/pa_original.csv.gz'}],
                    'Name': 'cp-ref-etl-output-bucket-dev', 'Prefix': 'source_prefix', 'MaxKeys': 1000,
                    'EncodingType': 'url', 'KeyCount': 2}

        with patch('botocore.client.BaseClient._make_api_call', return_value=response):
            try:
                validate_copy_count('source_bucket', 'source_prefix', 2, 'destination_bucket', 'destination_prefix')
            except ValueError:
                self.fail("Should not fail")

    def test_validate_copy_count_partial_copy(self):
        response = {'ResponseMetadata': {'RequestId': '0DA5ACACE26C14D9',
                                         'HTTPStatusCode': 200, 'RetryAttempts': 0},
                    'IsTruncated': False, 'Contents': [{'Key': 'source_prefix/pa_data_output_011.csv'},
                                                       {'Key': 'source_prefix/pa_original.csv.gz'}],
                    'Name': 'cp-ref-etl-output-bucket-dev', 'Prefix': 'source_prefix', 'MaxKeys': 1000,
                    'EncodingType': 'url', 'KeyCount': 2}

        with patch('botocore.client.BaseClient._make_api_call', return_value=response):
            with self.assertRaises(ValueError):
                validate_copy_count('source_bucket', 'source_prefix', 3, 'destination_bucket', 'destination_prefix')

    def test_validate_copy_count_unsuccessful_with_copy_failed(self):
        response = {'ResponseMetadata': {'HTTPStatusCode': 200, 'RetryAttempts': 0},
                    'IsTruncated': False, 'Name': 'cp-ref-etl-output-bucket-dev', 'Prefix': 'destination_prefix',
                    'MaxKeys': 1000, 'EncodingType': 'url', 'KeyCount': 0}

        with patch('botocore.client.BaseClient._make_api_call', return_value=response):
            with self.assertRaises(ValueError):
                validate_copy_count('source_bucket', 'source_prefix', 5, 'destination_bucket', 'destination_prefix')
