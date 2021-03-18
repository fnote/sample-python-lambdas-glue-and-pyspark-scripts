import boto3
import pymysql

ENV_PARAM_NAME = 'ENV'
CLUSTER_PARAM_NAME = 'cluster'
CLUSTER_N_OPCO_PARAM_NAME = 'cluster_opcos'
FILE_NAME_PARAM_NAME = 's3_object_key'
ETL_TIMESTAMP_PARAM_NAME = 'etl_timestamp'
ALLOCATED_JOB_COUNT_PARAM_NAME = 'allocated_job_count'
JOB_EXECUTION_STATUS_FETCH_QUERY = 'SELECT * FROM PRICE_ZONE_LOAD_JOB_EXECUTION_STATUS WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={} FOR UPDATE'
JOB_EXECUTION_STATUS_UPDATE_QUERY = 'UPDATE PRICE_ZONE_LOAD_JOB_EXECUTION_STATUS SET SUCCESSFUL_BUSINESS_UNITS = SUCCESSFUL_BUSINESS_UNITS + {} WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={}'
CLUSTER_LOAD_JOB_COUNT_FETCH_QUERY = 'SELECT RUNNING_LOAD_JOB_COUNT FROM PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS WHERE CLUSTER_ID = {} FOR UPDATE'
CLUSTER_LOAD_JOB_COUNT_UPDATE_QUERY = 'UPDATE PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS SET RUNNING_LOAD_JOB_COUNT = RUNNING_LOAD_JOB_COUNT - {} WHERE CLUSTER_ID  = {}'
TOTAL_BUSINESS_UNITS_COLUMN_NAME = 'TOTAL_BUSINESS_UNITS'
SUCCESSFUL_BUSINESS_UNITS_COLUMN_NAME = 'SUCCESSFUL_BUSINESS_UNITS'
FAILED_BUSINESS_UNITS_COLUMN_NAME = 'FAILED_BUSINESS_UNITS'

charset = 'utf8'
cursor_type = pymysql.cursors.DictCursor

def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys, WithDecryption=True)
    # print(response)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']
    if invalid_parameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalid_parameters))
    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']
    print(parameter_dictionary)
    return parameter_dictionary


def get_connection_details(env):
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_NAME'
    ssm_keys = [db_url, db_name, username, password]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    print(ssm_key_values)
    return {
        "db_endpoint": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name]
    }


def get_db_connection(env):
    connection_params = get_connection_details(env)
    return pymysql.connect(
        host=connection_params['db_endpoint'], user=connection_params['username'], password=connection_params['password'], db=connection_params['db_name'], charset=charset, cursorclass=cursor_type)


def lambda_handler(event, context):
    env = event[ENV_PARAM_NAME]
    cluster = event[CLUSTER_PARAM_NAME]
    file_name = event[FILE_NAME_PARAM_NAME]
    etl_timestamp = event[ETL_TIMESTAMP_PARAM_NAME]
    allocated_job_count = event[ALLOCATED_JOB_COUNT_PARAM_NAME]
    cluster_opcos = event[CLUSTER_N_OPCO_PARAM_NAME.format(cluster)]
    cluster_opco_count = len(cluster_opcos)
    database_connection = get_db_connection(env)
    should_backup = True
    try:
        cursor_object = database_connection.cursor()
        cursor_object.execute(JOB_EXECUTION_STATUS_FETCH_QUERY.format(file_name, etl_timestamp))
        result = cursor_object.fetchone()
        total_opco_count = int(result[TOTAL_BUSINESS_UNITS_COLUMN_NAME])
        successful_opco_count = int(result[SUCCESSFUL_BUSINESS_UNITS_COLUMN_NAME])
        failed_opco_count = int(result[FAILED_BUSINESS_UNITS_COLUMN_NAME])

        if failed_opco_count > 0 | successful_opco_count + cluster_opco_count != total_opco_count:
            should_backup = False

        cursor_object.execute(JOB_EXECUTION_STATUS_UPDATE_QUERY.format(cluster_opco_count, file_name, etl_timestamp))
        cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_FETCH_QUERY.format(cluster))
        cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_UPDATE_QUERY.format(allocated_job_count, cluster))
        database_connection.commit()
    except Exception as e:
        print(e)
        # TODO: handle this
    finally:
        database_connection.close()

    return {'shouldBackup': should_backup}


# test_event = {
#     's3_object_key': 'CE-PRCP-2318.csv.gz',
#     'etl_timestamp': 1614663941
# }