import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions

from move_s3_objects import copy_input_file, copy_objects_with_prefix, delete_object, delete_directory

if __name__ == "__main__":
    print("started PA data moving for archival and cleaning\n")
    args = getResolvedOptions(sys.argv, ['s3_input_bucket', 's3_input_file_key', 'etl_output_path_key', 'etl_timestamp',
                                         'INTERMEDIATE_S3_BUCKET', 'ARCHIVING_S3_BUCKET', 'backup_file_path'])

    s3_input_bucket = args['s3_input_bucket']
    s3_input_file_key = args['s3_input_file_key']
    etl_timestamp = args['etl_timestamp']
    etl_output_path_key = args['etl_output_path_key']
    intermediate_s3_bucket = args['INTERMEDIATE_S3_BUCKET']
    archiving_s3_bucket = args['ARCHIVING_S3_BUCKET']
    backup_file_path = args['backup_file_path']

    etl_time_object = datetime.fromtimestamp(int(etl_timestamp))

    archiving_path = backup_file_path

    input_file_destination_key = archiving_path + s3_input_file_key
    copy_input_file(s3_input_bucket, s3_input_file_key, archiving_s3_bucket, input_file_destination_key)

    metadata_file = '{}/additionalInfo.txt'.format(etl_output_path_key)
    copy_input_file(intermediate_s3_bucket, metadata_file, archiving_s3_bucket, archiving_path + 'additionalInfo.txt')
    opco_partitioned_path = archiving_path + 'partitioned/'
    copy_objects_with_prefix(intermediate_s3_bucket, etl_output_path_key, archiving_s3_bucket, opco_partitioned_path)

    # cleaning source files
    delete_object(s3_input_bucket, s3_input_file_key)
    delete_directory(source_bucket=intermediate_s3_bucket, prefix=etl_output_path_key)
