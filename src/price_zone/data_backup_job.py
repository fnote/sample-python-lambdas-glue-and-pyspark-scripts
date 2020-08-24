import sys
from datetime import datetime
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from move_s3_objects import copy_input_file, copy_objects_with_prefix, delete_object, delete_directory

if __name__ == "__main__":
    print("started Price Zone data moving for archival and cleaning\n")
    args = getResolvedOptions(sys.argv, ['s3_input_bucket', 's3_input_file_key', 'partitioned_files_key',
                                         'decompressed_file_path', 'etl_timestamp',
                                         'INTERMEDIATE_S3_BUCKET', 'ARCHIVING_S3_BUCKET'])

    s3_input_bucket = args['s3_input_bucket']
    s3_input_file_key = args['s3_input_file_key']
    etl_timestamp = args['etl_timestamp']
    partitioned_files_path = args['partitioned_files_key']
    decompressed_file_path = args['decompressed_file_path']
    intermediate_s3_bucket = args['INTERMEDIATE_S3_BUCKET']
    archiving_s3_bucket = args['ARCHIVING_S3_BUCKET']

    etl_time_object = datetime.fromtimestamp(int(etl_timestamp))

    archiving_path = 'price_zone/' + str(etl_time_object.year) + '/' + etl_time_object.strftime("%B") + '/' + str(
        etl_time_object.day) + '/etl_output_' + etl_timestamp + '/'

    decompressed_file_parsed_path = urlparse(decompressed_file_path, allow_fragments=False)
    decompressed_file_bucket = decompressed_file_parsed_path.netloc
    decompressed_file_key = decompressed_file_parsed_path.path.lstrip('/')

    input_file_destination_key = archiving_path + s3_input_file_key
    decompressed_file_destination_key = archiving_path + decompressed_file_key.split('/')[-1]
    copy_input_file(s3_input_bucket, s3_input_file_key, archiving_s3_bucket, input_file_destination_key)
    copy_input_file(decompressed_file_bucket, decompressed_file_key, archiving_s3_bucket, decompressed_file_destination_key)
    opco_partitioned_path = archiving_path + 'partitioned/'
    copy_objects_with_prefix(intermediate_s3_bucket, partitioned_files_path, archiving_s3_bucket, opco_partitioned_path)

    # cleaning source files
    delete_object(s3_input_bucket, s3_input_file_key)
    delete_object(decompressed_file_bucket, decompressed_file_key)
    delete_directory(source_bucket=intermediate_s3_bucket, prefix=partitioned_files_path)
