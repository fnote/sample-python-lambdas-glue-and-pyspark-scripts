import logging
import os
import pymysql
import boto3
from collections import Counter

OPCO_CLUSTER_MAPPINGS_QUERY = 'SELECT * FROM OPCO_CLUSTER_MAPPING WHERE OPCO_ID IN ({})'
CLUSTER_ID_COLUMN_NAME = 'CLUSTER_ID'
OPCO_ID_COLUMN_NAME = 'OPCO_ID'

charset = 'utf8'
cursor_type = pymysql.cursors.DictCursor


CLUSTER_ENV_VAR_NAME = 'CLUSTER'
MAX_LOAD_JOB_COUNT_KEY = 'MAX_LOAD_JOB_COUNT'
RUNNING_LOAD_JOB_COUNT_KEY = 'RUNNING_LOAD_JOB_COUNT'
OPCO_LIST_KEY = 'opcos_cluster_{}'
CLUSTER_LOAD_JOB_COUNT_QUERY = 'SELECT MAX_LOAD_JOB_COUNT, RUNNING_LOAD_JOB_COUNT FROM PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS WHERE CLUSTER_ID = {}'
CLUSTER_JOB_COUNT_UPDATE_QUERY = 'UPDATE PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS SET RUNNING_LOAD_JOB_COUNT = RUNNING_LOAD_JOB_COUNT + {} WHERE CLUSTER_ID  = {}'
JOB_COUNT_AFTER_UPDATE_QUERY = 'SELECT RUNNING_LOAD_JOB_COUNT FROM PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS WHERE CLUSTER_ID={}'


def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys)
    parameters = response['Parameters']
    invalidParameters = response['InvalidParameters']

    if invalidParameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalidParameters))

    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']

    return parameter_dictionary


def get_connection_details(env):
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_NAME'
    print(db_url, password, username, db_name)
    ssm_keys = [db_url, password, username, db_name]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    print(ssm_key_values)
    return {
        "db_url": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name]
    }

def get_db_connection(env):
    connection_params = get_connection_details(env)
    return pymysql.connect(
        host=connection_params['db_url'], user=connection_params['username'], password=connection_params['password'], db=connection_params['db_name'], charset=charset, cursorclass=cursor_type)


def lambda_handler(event, context):
    # cluster = os.environ[CLUSTER_ENV_VAR_NAME]
    # opco_list_for_cluster = event[OPCO_LIST_KEY.format(cluster)]
    # print(opco_list_for_cluster)
    # print(opco_list_for_cluster[0])
    # print(opco_list_for_cluster[1])
    # print(opco_list_for_cluster[2])
    # print(opco_list_for_cluster[3])
    opco_list_for_cluster = event['Input']
    cluster = int(event['cluster'])

    if len(opco_list_for_cluster) == 0:
        return {'nextStep': 'terminate'}
    else:
        required_job_count = len(opco_list_for_cluster)
        database_connection = get_db_connection("DEV")
        try:
            cursor_object = database_connection.cursor()
            cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_QUERY.format(cluster))
            job_count_result = cursor_object.fetchone()
            max_count = job_count_result[MAX_LOAD_JOB_COUNT_KEY]
            running_count = job_count_result[RUNNING_LOAD_JOB_COUNT_KEY]
            available_count = max_count - running_count

            print('Available count: {}'.format(available_count))
            print('Currently running count: {}'.format(running_count))

            if available_count == 0:
                return {'nextStep': 'Wait'}

            update_count = 0

            if available_count >= required_job_count:
                update_count = required_job_count
            else:
                update_count = available_count

            cursor_object.execute(CLUSTER_JOB_COUNT_UPDATE_QUERY.format(update_count, cluster))
            database_connection.commit()

            cursor_object.execute(JOB_COUNT_AFTER_UPDATE_QUERY.format(cluster))
            result_after_update = cursor_object.fetchone()

            print("Actual running count after update: {}".format(result_after_update[RUNNING_LOAD_JOB_COUNT_KEY]))
            return {'nextStep': 'Load All', 'allocatedJobCount': update_count}
        except Exception as e:
            # TODO: handle this appropriately
            print(e)
        finally:
            database_connection.close()


# out = lambda_handler({'opcos_cluster_1': ['001', '003', '005'], 'opcos_cluster_2': ['002', '004']}, {})
# out = lambda_handler({'opcos_cluster_1': [], 'opcos_cluster_2': ['002', '004']}, {})
# print(out)