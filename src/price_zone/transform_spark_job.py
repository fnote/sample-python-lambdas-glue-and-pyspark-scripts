import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from constants import CUST_NBR_LENGTH, SUPC_LENGTH, PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE, DATE_FORMAT_REGEX, \
    OUTPUT_DATE_FORMAT, INPUT_DATE_FORMAT
from pyspark.context import SparkContext
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType
from validator import validate_column, validate_column_length_less_than, validate_data_range, validate_date_format, \
    validate_date_time_field, validate_opcos, \
    remove_records_of_given_opcos

lambda_client = boto3.client('lambda')
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 's3_path', 'decompressed_file_path', 'partitioned_files_path', 'active_opcos',
                           'intermediate_s3_name', 'intermediate_directory_path', 'METADATA_LAMBDA', 'file_type'])
input_file_path = args['s3_path']
decompressed_file_path = args['decompressed_file_path']
partitioned_files_path = args['partitioned_files_path']
active_opcos = args['active_opcos']
file_type = args['file_type']
metadata_lambda = args['METADATA_LAMBDA']
intermediate_s3_name = args['intermediate_s3_name']
intermediate_directory_path = args['intermediate_directory_path']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

if file_type == 'gz':
    file_location = decompressed_file_path
else:
    file_location = input_file_path

datasourceDF = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("sep", ",") \
    .load(file_location)

datasource0 = DynamicFrame.fromDF(datasourceDF, glueContext, "datasource0")

# renaming columns and dropping off unnecessary columns
applyMapping1 = ApplyMapping.apply(frame=datasource0, mappings=[("co_nbr", "string", "opco_id", "string"),
                                                                ("supc", "string", "supc", "string"),
                                                                ("prc_zone", "string", "price_zone", "string"),
                                                                ("cust_nbr", "string", "customer_id", "string"),
                                                                ("eff_from_dttm", "string", "eff_from_dttm", "string")],
                                   transformation_ctx="applyMapping1")
sparkDF = applyMapping1.toDF()

# fetch active opcos
active_opco_id_list = active_opcos.split(',')

# validate data
invalid_opcos = []
invalid_opcos.extend(validate_column(sparkDF, 'customer_id'))
invalid_opcos.extend(validate_column(sparkDF, 'supc'))
invalid_opcos.extend(validate_column(sparkDF, 'price_zone'))
invalid_opcos.extend(validate_date_format(sparkDF, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT))

invalid_opcos.extend(validate_column_length_less_than(sparkDF, 'customer_id', CUST_NBR_LENGTH))
invalid_opcos.extend(validate_column_length_less_than(sparkDF, 'supc', SUPC_LENGTH))

# validate opcos
invalid_opcos.extend(validate_opcos(sparkDF, active_opco_id_list, 'opco_id'))

sparkDF = sparkDF.withColumn("price_zone", sparkDF["price_zone"].cast(IntegerType()))
invalid_opcos.extend(validate_data_range(sparkDF, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE))

sparkDF = sparkDF.withColumn('effective_date', to_timestamp(sparkDF['eff_from_dttm'], OUTPUT_DATE_FORMAT))
invalid_opcos.extend(validate_date_time_field(sparkDF, 'effective_date'))

validated_records = remove_records_of_given_opcos(sparkDF, invalid_opcos)

response = lambda_client.invoke(FunctionName=metadata_lambda, Payload=json.dumps({
    "intermediate_s3_name": intermediate_s3_name,
    "intermediate_directory_path": intermediate_directory_path,
    "failed_opcos": invalid_opcos,
    "received_records_count": sparkDF.count(),
    "received_valid_records_count": validated_records.count(),
}))

if validated_records.count() == 0:
    raise ValueError("There are no valid records to process")

convertedDynamicFrame = DynamicFrame.fromDF(validated_records, glueContext, "convertedDynamicFrame")

# drop eff_from_dttm
dropped_dynamicdataframe = DropFields.apply(frame=convertedDynamicFrame, paths=["eff_from_dttm"],
                                            transformation_ctx="dropped_dynamicdataframe")

# cast timestamp type of effective_date to string
casted_dynamicframe = dropped_dynamicdataframe.resolveChoice(specs=[('effective_date', 'cast:string')])

# save dataframe to s3, partitioned per OPCO with quotes removed from casted effective_date
datasink2 = glueContext.write_dynamic_frame.from_options(frame=casted_dynamicframe, connection_type="s3",
                                                         connection_options={"path": partitioned_files_path,
                                                                             "partitionKeys": ["opco_id"]},
                                                         format="csv", format_options={"quoteChar": -1},
                                                         transformation_ctx="datasink2")

job.commit()
