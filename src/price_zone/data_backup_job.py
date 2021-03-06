"""
Price zone etl data back up job
"""
# pylint: disable=import-error

import sys
from datetime import datetime
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from move_s3_objects import copy_input_file, copy_objects_with_prefix, delete_object, delete_directory

if __name__ == "__main__":
    print("started Price Zone data moving for archival and cleaning\n")
    args = getResolvedOptions(sys.argv,
                              ['s3_input_bucket',
                               's3_input_file_key',
                               'partitioned_files_key',
                               'decompressed_file_path',
                               'etl_timestamp',
                               'etl_output_path_key',
                               'intermediate_directory_path',
                               'INTERMEDIATE_S3_BUCKET',
                               'ARCHIVING_S3_BUCKET',
                               'backup_file_path',
                               'file_type'])

    s3_input_bucket = args['s3_input_bucket']
    s3_input_file_key = args['s3_input_file_key']
    etl_timestamp = args['etl_timestamp']
    etl_output_path_key = args['etl_output_path_key']
    partitioned_files_path = args['partitioned_files_key']
    decompressed_file_path = args['decompressed_file_path']
    intermediate_s3_bucket = args['INTERMEDIATE_S3_BUCKET']
    intermediate_directory_path = args['intermediate_directory_path']
    archiving_s3_bucket = args['ARCHIVING_S3_BUCKET']
    backup_file_path = args['backup_file_path']
    file_type = args['file_type']

    etl_time_object = datetime.fromtimestamp(int(etl_timestamp))

    archiving_path = backup_file_path

    decompressed_file_parsed_path = urlparse(decompressed_file_path, allow_fragments=False)
    decompressed_file_bucket = decompressed_file_parsed_path.netloc
    decompressed_file = decompressed_file_parsed_path.path.lstrip('/')

    input_file_destination_key = archiving_path + s3_input_file_key
    decompressed_file_path_key = archiving_path + decompressed_file.split('/')[-1]

    copy_input_file(s3_input_bucket, s3_input_file_key, archiving_s3_bucket, input_file_destination_key)

    # do this if gzip only
    if file_type == 'gz':
        copy_input_file(decompressed_file_bucket, decompressed_file, archiving_s3_bucket, decompressed_file_path_key)

    METADATA_FILE = '{}/additionalInfo.txt'.format(intermediate_directory_path)
    copy_input_file(decompressed_file_bucket, METADATA_FILE, archiving_s3_bucket, archiving_path + 'additionalInfo.txt')

    opco_partitioned_path = archiving_path + 'partitioned/'
    copy_objects_with_prefix(intermediate_s3_bucket, partitioned_files_path, archiving_s3_bucket, opco_partitioned_path)

    # cleaning source files
    delete_object(s3_input_bucket, s3_input_file_key)

    # do this if gzip only
    if file_type == 'gz':
        delete_object(decompressed_file_bucket, decompressed_file)

    delete_directory(source_bucket=intermediate_s3_bucket, prefix=partitioned_files_path)
    delete_directory(source_bucket=intermediate_s3_bucket, prefix=intermediate_directory_path)
