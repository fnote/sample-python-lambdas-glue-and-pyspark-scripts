import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import IntegerType
from validator import validate_column, validate_column_length_less_than, validate_column_length_equals, validate_data_range, validate_date_format, validate_and_get_as_date_time,validate_opcos
from constants import CUST_NBR_LENGTH, SUPC_LENGTH, PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE, DATE_FORMAT_REGEX, OUTPUT_DATE_FORMAT, INPUT_DATE_FORMAT, CO_NBR_LENGTH

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'decompressed_file_path', 'partitioned_files_path', 'ENV'])
decompressed_file_path = args['decompressed_file_path']
partitioned_files_path = args['partitioned_files_path']
environment = args['ENV']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


datasourceDF = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("sep", ",") \
    .load(decompressed_file_path)

datasource0 = DynamicFrame.fromDF(datasourceDF, glueContext, "datasource0")

# renaming columns and dropping off unnecessary columns
applyMapping1 = ApplyMapping.apply(frame=datasource0, mappings=[("co_nbr", "string", "opco_id", "string"),
                                                                ("supc", "string", "supc", "string"),
                                                                ("prc_zone", "string", "price_zone", "string"),
                                                                ("cust_nbr", "string", "customer_id", "string"),
                                                                ("eff_from_dttm", "string", "eff_from_dttm", "string")],
                                   transformation_ctx="applyMapping1")
sparkDF = applyMapping1.toDF()

#fetch active opcos
ssm = boto3.client('ssm')
opco_list_parameter_key = '/CP/DISCOUNT_SERVICE/' + environment + '/ACTIVE/BUSINESS/UNITS'
parameter = ssm.get_parameter(Name=opco_list_parameter_key, WithDecryption=False)
active_opco_id_list = parameter['Parameter']['Value'].split(",")

# validate data
validate_column(sparkDF, 'opco_id')
validate_column(sparkDF, 'customer_id')
validate_column(sparkDF, 'supc')
validate_column(sparkDF, 'price_zone')
validate_date_format(sparkDF, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

validate_column_length_less_than(sparkDF, 'customer_id', CUST_NBR_LENGTH)
validate_column_length_less_than(sparkDF, 'supc', SUPC_LENGTH)
validate_column_length_equals(sparkDF, 'opco_id', CO_NBR_LENGTH)

#validate opcos
validate_opcos(sparkDF, active_opco_id_list)

sparkDF = sparkDF.withColumn("price_zone", sparkDF["price_zone"].cast(IntegerType()))
validate_data_range(sparkDF, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

sparkDF = validate_and_get_as_date_time(sparkDF, 'eff_from_dttm', 'effective_date', OUTPUT_DATE_FORMAT)

convertedDynamicFrame = DynamicFrame.fromDF(sparkDF, glueContext, "convertedDynamicFrame")

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
