import boto3
import re
import MySQLdb
from pandas import DataFrame

def extract_opco_id(x):
     p = re.search('opco_id=(\d+?)/', x['Key'])
     return p and p.group(1)

def load_cluster_details_from_common_db():
    # Open database connection
    print('loading from common db')
    db = MySQLdb.connect("cp-ref-price-db-cluster-01-dev.cluster-c6xai0tt38eb.us-east-1.rds.amazonaws.com", "cpuser",
                         "a4T6XrIXJGwp",
                         "COMMON_DB")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    businessunits = ["001", "002"]
    # Prepare SQL query to INSERT a record into the database.
    sql2 = "SELECT ocm.BUSINESS_UNIT_NUMBER,ocm.CLUSTER_ID,ci.READER_ENDPOINT,ci.WRITER_ENDPOINT FROM OPCO_CLUSTER_MAPPINGS ocm INNER JOIN CLUSTER_INFO ci ON ocm.CLUSTER_ID = ci.CLUSTER_ID WHERE ocm.BUSINESS_UNIT_NUMBER IN(%s)"%businessunits

    print(sql2)

    sql = "SELECT SUPC,CUSTOMER_ID,EXPORTED_DATE,END_DATE,PRICE from EXCEPTION"
    try:
        # Execute the SQL command
        cursor.execute(sql2)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        print(results)


    except:
        print("Error: unable to fetch data")

    # disconnect from server
    db.close()


def lambda_handler(event, context):
    client = boto3.client('s3')

    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=event['intermediate_s3_name'], Prefix=event['partitioned_files_key'])

    opco_id_set = set()
    for page in pages:
        opco_ids = map(extract_opco_id, page['Contents'])
        for opco_id in set(opco_ids):
            opco_id_set.add(opco_id)

    #retrun cluster a and cluster b
    #call commnon db with the opco id list here and see which opcos belong to which cluster

    print(list(opco_id_set))
    return list(opco_id_set)

load_cluster_details_from_common_db()