import logging
import os
from collections import Counter

import boto3
from botocore.config import Config
import pymysql
from datetime import datetime

# file name , etl, total bbusiness unit count , success count, failed count , file type ,  failed opco ids, success opco ids , status, record count ,start time ,end time,partial load
EXECUTION_STATUS_INSERT_QUERY = 'INSERT INTO LOAD_JOB_EXECUTION_STATUS (FILE_NAME,ETL_TIMESTAMP, FILE_TYPE,STATUS,TOTAL_ACTIVE_OPCO_COUNT,SUCCESSFUL_ACTIVE_OPCO_COUNT,FAILED_ACTIVE_OPCO_COUNT,SUCCESSFUL_ACTIVE_OPCO_IDS,FAILED_OPCO_IDS,TOTAL_RECORD_COUNT,INVALID_RECORD_COUNT,PARTIAL_LOAD,RECEIVED_OPCOS,START_TIME,END_TIME) VALUES ("{}", "{}","{}","{}", 0, 0, 0 ,"","",0,0,"{}","0","{}","")'
RECORD_EXIST_CHECK_QUERY = 'SELECT * FROM LOAD_JOB_EXECUTION_STATUS WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={}'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
charset = 'utf8'
cursor_type = pymysql.cursors.DictCursor

# adaptive - Retries with additional client side throttling.
config = Config(
   retries={
      'max_attempts': 50,
      'mode': 'adaptive'
   }
)

def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm', config=config)
    response = client_ssm.get_parameters(Names=keys, WithDecryption=True)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']
    if invalid_parameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalid_parameters))
    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']
    logger.info('ssm parameter dictionary returned: %s' % parameter_dictionary)
    return parameter_dictionary


def get_connection_details_and_max_concurrency(env):
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_NAME'
    max_concurrency_ssm_key = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/MAX_CONCURRENCY'
    ssm_keys = [db_url, db_name, username, password, max_concurrency_ssm_key]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    logger.info('GetParameter called for ssm key: %s, %s, %s' % (db_url, db_name, max_concurrency_ssm_key))
    return {
        "db_endpoint": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name],
        "max_concurrency": ssm_key_values[max_concurrency_ssm_key]
    }


def get_db_connection(connection_params):
    return pymysql.connect(
        host=connection_params['db_endpoint'], user=connection_params['username'], password=connection_params['password'], db=connection_params['db_name'], charset=charset, cursorclass=cursor_type)


def str_to_bool_int(s):
    if s == 'True':
         return 1
    elif s == 'False':
         return 0
    else:
         raise ValueError


def lambda_handler(event, context):
    logger.info("Received event:")
    logger.info(event)
    status = 'RUNNING'
    file_progress_status = "RUNNING"

    step_function = boto3.client('stepfunctions', config=config)

    step_functionArn = event['stepFunctionArn']
    step_function_execution_id = event['stepFunctionExecutionId']
    etl_timestamp = event['etl_timestamp']
    file_name = event['s3_input_file_key']
    file_type = event['file_prefix']
    partial_load_string = event['partial_load']
    partial_load = str_to_bool_int(partial_load_string)
    env = os.environ['env']

    # get allowed concurrent step function execution amount and common db connection parameters
    params = get_connection_details_and_max_concurrency(env)
    database_connection = get_db_connection(params)
    ALLOWED_CONCURRENT_EXECUTIONS = int(params['max_concurrency'])

    if ALLOWED_CONCURRENT_EXECUTIONS == 0:
        error_msg = 'Received illegal value for Price Zone ETL workFlow maximum concurrency: {}' \
            .format(ALLOWED_CONCURRENT_EXECUTIONS)
        raise ValueError(error_msg)

    paginator = step_function.get_paginator('list_executions')
    pages = paginator.paginate(stateMachineArn=step_functionArn, statusFilter=status)
    logger.info('paginator:%s  pages:%s' % (paginator, pages))
    logger.info('Retrieved execution list for step function:%s with execution status:%s' % (step_functionArn, status))

    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        cursor_object = database_connection.cursor()
        cursor_object.execute(RECORD_EXIST_CHECK_QUERY.format(file_name, etl_timestamp))
        result = cursor_object.fetchone()
        logger.info('Retrieved record details from status table and the result :%s' % result)

        if result == None:
            res = cursor_object.execute(EXECUTION_STATUS_INSERT_QUERY.format(file_name, etl_timestamp, file_type, file_progress_status, partial_load, start_time))
            logger.info('insert query results :%s' % res)

        database_connection.commit()
    except Exception as e:
        logger.error(e)
        raise e
        # TODO: handle this

    finally:
        database_connection.close()
    #identify the file and etl timestamp here , avoid creating new records in db for retry occasions
    #if file name and etl exist update else insert

    execution_dictionary = {}
    for page in pages:
        logger.info(page)
        for execution in page['executions']:
            execution_arn = execution['executionArn']
            execution_dictionary[execution_arn] = execution['startDate'].timestamp()

    sorted_start_time_list = sorted(list(execution_dictionary.values()))

    shouldWait = True

    if step_function_execution_id in execution_dictionary:
        step_function_start_time = execution_dictionary.get(step_function_execution_id)
        step_function_wait_index = sorted_start_time_list.index(step_function_start_time) + 1
        logger.info("Current execution index: %d for  step function execution Id:%s with start time:%6f "
                    % (step_function_wait_index, step_function_execution_id, step_function_start_time))

        if step_function_wait_index <= ALLOWED_CONCURRENT_EXECUTIONS:
            start_time_counter = Counter(sorted_start_time_list)
            if start_time_counter[step_function_start_time] != 1:  # contains duplicates for same start time
                logger.info("contains multiple execution ids with same start time: %.6f and number of entries %d"
                            % (step_function_start_time, start_time_counter[step_function_start_time]))
                execution_id_list = []
                for key, value in execution_dictionary.items():
                    if value == step_function_start_time:
                        execution_id_list.append(key)

                # sort from execution Id
                sorted_execution_id_list = sorted(execution_id_list)
                logger.info("Execution ids containing same start time: ")
                logger.info(sorted_execution_id_list)
                step_function_wait_index = step_function_wait_index + sorted_execution_id_list.index(
                    step_function_execution_id)
                if step_function_wait_index <= ALLOWED_CONCURRENT_EXECUTIONS:
                    logger.info("New Execution index:%d for execution id:%s. Hence can proceed"
                                % (step_function_wait_index, step_function_execution_id))
                    shouldWait = False

            else:
                logger.info("Does not contain duplicate start time values for %.6f. Hence execution id:%s "
                            "with list index %d can proceed " % (step_function_start_time,
                                                                 step_function_execution_id,
                                                                 step_function_wait_index))
                shouldWait = False
        else:
            logger.info("Step function execution id:%s with start time:%.6f has list index %d. Allowed concurrency %d"
                        % (step_function_execution_id, step_function_start_time, step_function_wait_index,
                           ALLOWED_CONCURRENT_EXECUTIONS))
    else:
        logger.info("could not locate step function execution Id %s in the received execution list"
                    % step_function_execution_id)

    return {
        'shouldWait': shouldWait
    }
