import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import substring, length, col, expr


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={'paths': ["s3://cp-ref-price-poc-bucket/_fin_cust_pz_df.csv.gz"], "compressionType":"gzip"},format="csv", format_options={"separator": ",", 'withHeader': True}, transformation_ctx = "datasource0")


#drop fields from the dataframs
filteredDataframe = DropFields.apply(frame=datasource0, paths=["lvl_model_id","leaf_node_id","ks2_id","prc_zone_target"], transformation_ctx = "filteredDataframe")

#renaming columns
applyMapping1 = ApplyMapping.apply(frame=filteredDataframe, mappings =[("co_cust_nbr", "bigint", "co_cust_nbr", "string"), ("supc", "bigint", "supc", "string"), ("prc_zone", "bigint", "price_zone", "string")], transformation_ctx = "applyMapping1")
sparkDF = applyMapping1.toDF()

#creating new dataframe containing customer_id from co_cust_nbr
sparkDF = sparkDF.withColumn("customer_id", substring(col("co_cust_nbr"), -6, 6))

#creating new dataframe containing opco_id from co_cust_nbr
sparkDF = sparkDF.withColumn("opco_id", expr("substring(co_cust_nbr, 0, length(co_cust_nbr)-6)"))
convertedDynamicFrame = DynamicFrame.fromDF(sparkDF, glueContext, "convertedDynamicFrame")

#drop co_cust_nbr
customer_nb_dropped_dynamicdataframe = DropFields.apply(frame = convertedDynamicFrame, paths = ["co_cust_nbr"], transformation_ctx = "customer_nb_dropped_dynamicdataframe")

#save dataframe to s3, partitioned per OPCO
datasink2 = glueContext.write_dynamic_frame.from_options(frame=customer_nb_dropped_dynamicdataframe, connection_type="s3", connection_options={"path": "s3://cp-ref-price-poc-bucket/output/glue_output_partioned/", "partitionKeys": ["opco_id"]}, format="csv", transformation_ctx="datasink2")

job.commit()