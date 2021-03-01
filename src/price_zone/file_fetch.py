import boto3
import re
import MySQLdb
from pandas import DataFrame

def return_results(opco_cluster_map):
    resultant_json = {
        "cluster_a": opco_cluster_map[0],
        "cluster_b": opco_cluster_map[1]
    }

    return resultant_json

def filter_to_two_cluster(df):
    opco_cluster_mapping = []
    df_cluster_1 = df[df['cluster'] == 1]
    df_cluster_2 = df[df['cluster'] == 2]
    print(df_cluster_1)
    print(df_cluster_2)
    print(df)

    cluster_1_opco_list = df_cluster_1['opco'].tolist()
    cluster_2_opco_list = df_cluster_2['opco'].tolist()

    opco_cluster_mapping.append(cluster_1_opco_list)
    opco_cluster_mapping.append(cluster_2_opco_list)
    print(opco_cluster_mapping)
    return opco_cluster_mapping


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

    businessunits = ["001", "002","003","004"]
    joined_string = ",".join(businessunits)
    print(joined_string)
    # Prepare SQL query to INSERT a record into the database.
    sql2 = "SELECT ocm.BUSINESS_UNIT_NUMBER,ocm.CLUSTER_ID,ci.READER_ENDPOINT,ci.WRITER_ENDPOINT FROM OPCO_CLUSTER_MAPPINGS ocm INNER JOIN CLUSTER_INFO ci ON ocm.CLUSTER_ID = ci.CLUSTER_ID WHERE ocm.BUSINESS_UNIT_NUMBER IN(%s)"%joined_string

    print(sql2)

    sql = "SELECT SUPC,CUSTOMER_ID,EXPORTED_DATE,END_DATE,PRICE from EXCEPTION"
    try:
        # Execute the SQL command
        cursor.execute(sql2)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        print(results)
        df = DataFrame(results)
        print(df)
        df.columns = ['opco','cluster', 'reader', 'writer']
        print(df)

    except:
        print("Error: unable to fetch data")

    # disconnect from server
    db.close()
    return df


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

records_from_db_as_df = load_cluster_details_from_common_db()
print(records_from_db_as_df)
cluster_opco_mapping = filter_to_two_cluster(records_from_db_as_df)
final_result = return_results(cluster_opco_mapping)
print(final_result)
