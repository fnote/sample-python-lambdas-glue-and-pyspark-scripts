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
LOAD_JOB_MAXIMUM_CONCURRENCY_SSM_KEY = '/CP/{}/ETL/REF_PRICE/PRICE_ZONE/LOAD_JOB/CLUSTER_{}/MAX_CONCURRENCY'
OPCO_LIST_KEY = 'opcos_cluster_{}'
CLUSTER_LOAD_JOB_COUNT_QUERY = 'SELECT MAX_LOAD_JOB_COUNT, RUNNING_LOAD_JOB_COUNT FROM PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS WHERE CLUSTER_ID = {}'
CLUSTER_JOB_COUNT_UPDATE_QUERY = 'UPDATE PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS SET RUNNING_LOAD_JOB_COUNT = RUNNING_LOAD_JOB_COUNT + {} WHERE CLUSTER_ID  = {}'
JOB_COUNT_AFTER_UPDATE_QUERY = 'SELECT RUNNING_LOAD_JOB_COUNT FROM PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS WHERE CLUSTER_ID={}'
CLUSTER_LOAD_JOB_COUNT_UPDATE_QUERY = 'UPDATE PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS SET RUNNING_LOAD_JOB_COUNT = RUNNING_LOAD_JOB_COUNT + {} WHERE CLUSTER_ID  = {}'


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

def get_value_from_ssm(key):
    params_response = get_values_from_ssm([key])
    return params_response[key]

def lambda_handler(event, context):

    opco_list_for_cluster = event['Input']
    print(opco_list_for_cluster)
    cluster = int(event['cluster'])
    env = event['ENV']


    if len(opco_list_for_cluster) == 0:
        return {'nextStep': 'terminate'}
    else:
        required_job_count = len(opco_list_for_cluster)
        database_connection = get_db_connection(env)
        try:
            cursor_object = database_connection.cursor()
            cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_QUERY.format(cluster))
            job_count_result = cursor_object.fetchone()
            max_count = job_count_result[MAX_LOAD_JOB_COUNT_KEY]
            running_count = job_count_result[RUNNING_LOAD_JOB_COUNT_KEY]
            available_count = max_count - running_count

            print('Available count: {}'.format(available_count))
            print('Currently running count: {}'.format(running_count))

            max_load_job_concurrency = int(
                get_value_from_ssm(LOAD_JOB_MAXIMUM_CONCURRENCY_SSM_KEY.format(env, cluster)))
            # max_load_job_concurrency = 2

            if available_count == 0:
                cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_UPDATE_QUERY.format(0, cluster))
                database_connection.commit()
                return {'nextStep': 'Wait'}

            update_count = 0

            #more jobs available
            if available_count >= max_load_job_concurrency:
                #if no of opcos less than max update by number opco
                if required_job_count < max_load_job_concurrency:
                    update_count = required_job_count
                else:
                    #else update by max count
                    update_count = max_load_job_concurrency
            elif available_count >= required_job_count:
                update_count = required_job_count

            if update_count == 0:
                cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_UPDATE_QUERY.format(0, cluster))
                database_connection.commit()
                return {'nextStep': 'Wait'}

            #update table
            cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_UPDATE_QUERY.format(update_count, cluster))
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