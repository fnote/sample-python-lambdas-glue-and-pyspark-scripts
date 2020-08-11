import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions

from move_s3_objects import move_input_file, move_objects_with_prefix

if __name__ == "__main__":
    print("started PA data moving for archival and cleaning\n")
    args = getResolvedOptions(sys.argv, ['s3_path', 'etl_output_path_key', 'etl_timestamp', 'INTERMEDIATE_S3_BUCKET',
                                         'ARCHIVING_S3_BUCKET'])

    input_file_path = args['s3_path']
    etl_timestamp = args['etl_timestamp']
    etl_output_path_key = args['etl_output_path_key']
    intermediate_s3_bucket = args['INTERMEDIATE_S3_BUCKET']
    archiving_s3_bucket = args['ARCHIVING_S3_BUCKET']

    etl_time_object = datetime.fromtimestamp(int(etl_timestamp))

    archiving_path = 'pa/' + str(etl_time_object.year) + '/' + etl_time_object.strftime("%B") + '/' + str(
        etl_time_object.day) + '/etl_output_' + etl_timestamp + '/'

    move_input_file(input_file_path, archiving_s3_bucket, archiving_path)
    opco_partitioned = archiving_path + 'partitioned/'
    move_objects_with_prefix(intermediate_s3_bucket, etl_output_path_key, archiving_s3_bucket, opco_partitioned)
