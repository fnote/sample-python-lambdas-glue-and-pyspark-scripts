import re

import boto3
import pymysql

OPCO_CLUSTER_MAPPINGS_QUERY = 'SELECT * FROM OPCO_CLUSTER WHERE OPCO_ID IN ({})'
# file name , etl, total business unit count , success count, failed count , file type ,  failed opco ids, success opco ids , status, record count ,start time ,end time,partial load
JOB_EXECUTION_STATUS_UPDATE_QUERY = 'UPDATE LOAD_JOB_EXECUTION_STATUS SET TOTAL_ACTIVE_OPCO_COUNT = {} ,RECEIVED_OPCOS = "{}" WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={}'
CLUSTER_ID_COLUMN_NAME = 'CLUSTER_ID'
OPCO_ID_COLUMN_NAME = 'OPCO_ID'
ENVIRONMENT_PARAM_NAME = 'ENV'
FILE_NAME_PARAM_NAME = 's3_object_key'
ETL_TIMESTAMP_PARAM_NAME = 'etl_timestamp'
CLUSTER_1_OPCO_KEY = 'cluster_01'
CLUSTER_2_OPCO_KEY = 'cluster_02'

charset = 'utf8'
cursor_type = pymysql.cursors.DictCursor


def update_job_execution_status(env, file_name, etl_timestamp, opco_count, opco_list):
    print('update job execution status')
    database_connection = get_db_connection(env)
    try:
        cursor_object = database_connection.cursor()
        cursor_object.execute(JOB_EXECUTION_STATUS_UPDATE_QUERY.format(opco_count, opco_list, file_name, etl_timestamp))
        result = cursor_object.fetchall()
        print(result)
        database_connection.commit()
    except Exception as e:
        print(e)
        raise e
    finally:
        database_connection.close()


def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']

    if invalid_parameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalid_parameters))

    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']

    return parameter_dictionary


def get_connection_details(env):
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_NAME'
    ssm_keys = [db_url, password, username, db_name]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    return {
        "db_url": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name]
    }


def get_db_connection(env):
    connection_params = get_connection_details(env)
    return pymysql.connect(
        host=connection_params['db_url'], user=connection_params['username'], password=connection_params['password'],
        db=connection_params['db_name'], charset=charset, cursorclass=cursor_type)


def separate_opcos_by_cluster(mappings, active_opco_list):
    cluster_01_opcos = []
    cluster_02_opcos = []
    invalid_or_inactive_opcos = []
    for mapping in mappings:
        cluster_id = mapping[CLUSTER_ID_COLUMN_NAME]
        opco_id = mapping[OPCO_ID_COLUMN_NAME]
        if cluster_id == '01' and opco_id in active_opco_list:
            cluster_01_opcos.append(opco_id)
        elif cluster_id == '02' and opco_id in active_opco_list:
            cluster_02_opcos.append(opco_id)
        else:
            invalid_or_inactive_opcos.append(opco_id)

    resultant_json = {
        "cluster_01": cluster_01_opcos,
        "cluster_02": cluster_02_opcos,
        "invalid_or_inactive_opco_list": invalid_or_inactive_opcos
    }

    print("cluster 01 opcos: %s , cluster 02 opcos: %s , invalid or inactive opcos: %s" % (
        cluster_01_opcos, cluster_02_opcos, invalid_or_inactive_opcos))
    return resultant_json


def extract_opco_id(x):
    p = re.search('opco_id=(\d+?)/', x['Key'])
    return p and p.group(1)


def get_opco_cluster_mapping(opcos, env):
    database_connection = get_db_connection(env)
    try:
        cursor_object = database_connection.cursor()
        query = OPCO_CLUSTER_MAPPINGS_QUERY.format(opcos)
        print(query)
        cursor_object.execute(query)
        result = cursor_object.fetchall()
        return result
    except Exception as e:
        print(e)
        raise e
    finally:
        database_connection.close()


def lambda_handler(event, context):
    # read file type here
    client = boto3.client('s3')
    environment = event[ENVIRONMENT_PARAM_NAME]
    file_name = event[FILE_NAME_PARAM_NAME]
    etl_timestamp = event[ETL_TIMESTAMP_PARAM_NAME]
    active_opcos = event['active_opcos']
    active_opco_id_list = active_opcos.split(',')

    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=event['intermediate_s3_name'], Prefix=event['partitioned_files_key'])

    opco_id_set = set()
    for page in pages:
        opco_ids = map(extract_opco_id, page['Contents'])
        for opco_id in set(opco_ids):
            opco_id_set.add(opco_id)

    print(list(opco_id_set))
    opco_list = list(opco_id_set)
    joined_opco_list_string = ",".join(opco_list)

    opco_cluster_mappings = get_opco_cluster_mapping(joined_opco_list_string, environment)

    separated_opcos = separate_opcos_by_cluster(opco_cluster_mappings, active_opco_id_list)

    valid_opco_count = len(separated_opcos[CLUSTER_1_OPCO_KEY]) + len(separated_opcos[CLUSTER_2_OPCO_KEY])

    # update status table
    # 5 attributes, file type , success opcos , failed opcos , record count status start time end time
    update_job_execution_status(environment, file_name, etl_timestamp, valid_opco_count, joined_opco_list_string)

    return separated_opcos
