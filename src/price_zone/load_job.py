"""
Price zone etl load job
"""
# pylint: disable=import-error,too-few-public-methods,redefined-outer-name,no-else-return,no-else-raise,line-too-long

import base64
import sys
import threading
import time
from functools import reduce
from queue import Queue

import boto3
import pymysql
import sqlalchemy as sqlalchemy
from awsglue.utils import getResolvedOptions
from sqlalchemy.pool import QueuePool

lambda_client = boto3.client('lambda')

QUERY_FOR_TABLES = 'SELECT * FROM settings WHERE setting = (%s)'
GLUE_CONNECTION_NAME = 'cp-ref-etl-common-connection-{}-cluster-{}'

CHARSET = 'utf8'
CursorType = pymysql.cursors.DictCursor

JOB_EXECUTION_STATUS_FETCH_QUERY = 'SELECT RECEIVED_OPCOS FROM PRICE_ZONE_LOAD_JOB_EXECUTION_STATUS WHERE PARTIAL_LOAD={} AND STATUS="{}" FOR UPDATE'


def get_values_from_ssm(keys):
    """
    fetch ssm params from aws parameter store
    """
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys, WithDecryption=True)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']
    if invalid_parameters:
        raise KeyError(
            'Found invalid ssm parameter keys:' +
            ','.join(invalid_parameters))
    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']
    print(parameter_dictionary)
    return parameter_dictionary


def get_common_db_connection_details(env):
    """
     group ssm keys to be sent to parameter store to get common db credentials
    """
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_NAME'
    soft_validation_on_future_table_loading = '/CP/' + env + \
        '/ETL/REF_PRICE/PRICE_ZONE' '/FULL_EXPORT_LOADING/SOFT_VALIDATION'
    ssm_keys = [db_url, db_name, username, password,
                soft_validation_on_future_table_loading]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    print(ssm_key_values)
    return {
        "db_endpoint": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name],
        "soft_validation": ssm_key_values[soft_validation_on_future_table_loading]}


def get_common_db_connection(connection_params):
    """
        get a connection to common db passing relevant credentials
    """
    return pymysql.connect(
        host=connection_params['db_endpoint'],
        user=connection_params['username'],
        password=connection_params['password'],
        db=connection_params['db_name'],
        charset=CHARSET,
        cursorclass=CursorType)


def _execute_load(pool, queue, database, table, thread_errors):
    while not queue.empty():
        s3_file_path = queue.get()
        try:
            table_with_database_name = database + Configuration.DOT + table
            load_timestamp = str(int(time.time()))
            load_qry = "LOAD DATA FROM S3 '" + s3_file_path + "'" \
                                                              "REPLACE INTO TABLE " + table_with_database_name + \
                       " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' " \
                       "IGNORE 1 LINES (@supc,@price_zone,@customer_id,@effective_date) SET " \
                       "SUPC=@supc," \
                       "CUSTOMER_ID=@customer_id," \
                       "EFFECTIVE_DATE=@effective_date," \
                       "PRICE_ZONE=@price_zone," \
                       "ARRIVED_TIME=" + data_arrival_timestamp + "," \
                                                                  "UPDATED_TIME=" + load_timestamp + ";"

            connection = pool.connect()
            print(
                "Populating price zone data from file: %s to table %s with load time %s\n" %
                (s3_file_path, table_with_database_name, load_timestamp))
            connection.execute(load_qry)
            print(
                "Completed loading price zone data from s3 %s to table %s\n" %
                (s3_file_path, table_with_database_name))
            connection.close()
        except Exception as e:
            print(
                "Error occurred when trying to file: %s, error: %s" %
                (s3_file_path, repr(e)))
            thread_errors.append(repr(e))
            raise e


def load_data(dbconfigs, opco_id, bucketname, partitioned_files_path):
    """
            load price zone data in to the correct opco db and table
            """
    prefix = partitioned_files_path + Configuration.OUTPUT_PATH_PREFIX + opco_id
    output_files = list_files_in_s3(bucketname, prefix)
    dbconfigs['database'] = Configuration.DATABASE_PREFIX + opco_id
    pool = __create_db_engine(dbconfigs)
    queue = Queue()
    for file in output_files:
        prefix_path = prefix + '/'
        if file['Key'] != prefix_path:
            s3_file_path = "s3://" + bucketname + "/" + file['Key']
            queue.put(s3_file_path)

    thread_errors = []
    threads = []
    for i in range(0, 1):
        print(i)
        threads.append(
            threading.Thread(
                target=_execute_load,
                args=(
                    pool,
                    queue,
                    dbconfigs['database'],
                    dbconfigs['table'],
                    thread_errors)))

    for thr in threads:
        thr.start()

    for thr in threads:
        thr.join()

    if len(thread_errors) > 0:
        print(thread_errors)
        raise Exception(thread_errors)


def get_new_connection(host, user, decrypted, db):
    """
     get a new connection to the db s to which the data is being loaded
    """
    return pymysql.connect(host=host, user=user, password=decrypted, db=db)


def _retrieve_conection_details(cluster_id):
    glue = boto3.client('glue', region_name='us-east-1')

    response = glue.get_connection(
        Name=GLUE_CONNECTION_NAME.format(
            environment, cluster_id))

    connection_properties = response['Connection']['ConnectionProperties']
    URL = connection_properties['JDBC_CONNECTION_URL']
    url_list = URL.split("/")

    host = "{}".format(url_list[-2][:-5])
    port = url_list[-2][-4:]
    user = "{}".format(connection_properties['USERNAME'])
    pwd = "{}".format(connection_properties['ENCRYPTED_PASSWORD'])

    session = boto3.session.Session()
    client = session.client('kms', region_name='us-east-1')
    decrypted = client.decrypt(
        CiphertextBlob=bytes(base64.b64decode(pwd))
    )
    return {
        'username': user,
        'password': decrypted['Plaintext'].decode("utf-8"),
        'host': host,
        'port': int(port),
        "decrypted": decrypted
    }


def get_values_from_ssm(keys):
    """
            fetch ssm params from parameter store
            """
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']

    if invalid_parameters:
        raise KeyError(
            'Found invalid ssm parameter keys:' +
            ','.join(invalid_parameters))

    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']

    return parameter_dictionary


def get_active_and_future_tables(table, db_configs):
    """
        method to get the active and future table names
    """
    # from common db
    database_connection = get_new_connection(
        db_configs['host'],
        db_configs['username'],
        db_configs['password'],
        db_configs['database'])

    try:
        cursor_object = database_connection.cursor()

        sql = "SELECT TABLE_NAMES FROM PRICE_ZONE_MASTER_DATA WHERE TABLE_TYPE= '%s'" % table
        print(sql)
        cursor_object.execute(sql)
        result = cursor_object.fetchall()
        print(result)
        return result
    except Exception as e:
        print(e)
        raise e
    finally:
        database_connection.close()


def check_table_is_empty(table, db_configs):
    """
        method to check if a given table is empty
    """
    print('check if tables are empty')
    # here not common db connection ,connection to ref dbs has to be taken here
    # use the passed db config since it contains which table
    database_connection = get_new_connection(
        db_configs['host'],
        db_configs['username'],
        db_configs['password'],
        db_configs['database'])

    try:
        cursor_object = database_connection.cursor()
        sql = "SELECT * FROM %s LIMIT 1" % table
        print(sql)
        cursor_object.execute(sql)
        result = cursor_object.fetchall()
        print(result)
        return result
    except Exception as e:
        print(e)
        raise e
    finally:
        database_connection.close()


def update_table_effective_dates(db_configs, effective_date):
    """
     method to update the effective date in the price zone master data table
    """
    print('update the effective dates in price zone master data table')

    database_connection = get_new_connection(
        db_configs['host'],
        db_configs['username'],
        db_configs['password'],
        db_configs['database'])

    try:
        cursor_object = database_connection.cursor()

        # '0000-00-00 00:00:00' this is not a valid date handle this
        effective_date_of_latest_records = effective_date.strftime(
            "%Y-%m-%d %H:%M:%S")
        update_sql = "UPDATE PRICE_ZONE_MASTER_DATA SET EFFECTIVE_DATE ='%s' WHERE TABLE_TYPE = '%s'" % (
            effective_date_of_latest_records, 'FUTURE')
        print(update_sql)
        cursor_object.execute(update_sql)
        result = cursor_object.fetchall()

        print(result)
        return result
    except Exception as e:
        print(e)
        raise e
    finally:
        database_connection.commit()
        database_connection.close()


def get_effective_date(table, db_configs):
    """
     method to get the smallest date from future table to determine the effective date
    """
    database_connection = get_new_connection(
        db_configs['host'],
        db_configs['username'],
        db_configs['password'],
        db_configs['database'])
    try:
        cursor_object = database_connection.cursor()
        sql = "SELECT min(EFFECTIVE_DATE) FROM %s" % table
        print(sql)
        cursor_object.execute(sql)
        result = cursor_object.fetchall()
        print(result)
        print(result[0][0])
        return result
    except Exception as e:
        print(e)
        raise e
    finally:
        database_connection.close()


def check_for_full_exports_in_progress(common_db_connection_params, opco_id):
    """
        method to check if a full export is in progress for a given opco while loading a partial load
    """
    opcos_in_full_export = []
    database_connection = get_common_db_connection(
        connection_params=common_db_connection_params)
    try:
        cursor_object = database_connection.cursor()
        cursor_object.execute(
            JOB_EXECUTION_STATUS_FETCH_QUERY.format(
                0, "IN_PROGRESS"))
        results = cursor_object.fetchall()
        for result in results:
            opcos_in_full_export.append(result['RECEIVED_OPCOS'].split(','))
        flat_opco_list_in_export = []
        if opcos_in_full_export:
            flat_opco_list_in_export = reduce(
                list.__add__, opcos_in_full_export)
        if opco_id in flat_opco_list_in_export:
            print('current loading opco present in a full export in progress')
            return True
        else:
            print('current loading opco not present in full export in progress')
            return False

    except Exception as e:
        print(e)
        raise e
    finally:
        database_connection.close()


def str_to_bool(received_string_value):
    """
        method to convert string true false to bool true false
    """
    if received_string_value == 'True':
        return True
    elif received_string_value == 'False':
        return False
    else:
        raise ValueError


def find_tables_to_load(
        partial_load,
        env,
        opco_id,
        intermediate_s3,
        partitioned_files_key):
    """
        method to find tables and start loading
    """
    db_configs = _retrieve_conection_details(cluster_id)
    db_configs['database'] = Configuration.DATABASE_PREFIX + opco_id

    # get connection params for common db and validation type
    connection_params = get_common_db_connection_details(env)

    if partial_load:
        active_table = get_active_and_future_tables("ACTIVE", db_configs)
        future_table = get_active_and_future_tables("FUTURE", db_configs)

        # find active table from db
        active_table_name = active_table[0][0]
        future_table_name = future_table[0][0]

        # load data to active table
        db_configs['table'] = active_table_name
        load_data(db_configs, opco_id, intermediate_s3, partitioned_files_key)

        # check whether future table is empty
        db_configs['table'] = future_table_name
        future_table_query_result = check_table_is_empty(
            future_table_name, db_configs)
        print(future_table_query_result)

        if len(future_table_query_result) == 0:
            # If future table empty -> stop
            # Check if full export is in progress for that opco if so load to
            # future too
            opco_available_in_full_export = check_for_full_exports_in_progress(
                connection_params, opco_id)

            if opco_available_in_full_export:
                print(
                    'partial load , future table is empty and full export is in progress for the current opco, therefore load the future table')
                db_configs['table'] = future_table_name
                load_data(
                    db_configs,
                    opco_id,
                    intermediate_s3,
                    partitioned_files_key)
            else:
                # opco not available in in progress full export or no full
                # exports are currently running
                print(
                    'partial load and the future table is empty and full export is not in progress or current opco not in full export, therefore stop loading process')

        else:
            # future table not empty , therefore load future table
            db_configs['table'] = future_table_name
            print(
                'partial load and the future table is not empty, therefore load the future table')
            load_data(
                db_configs,
                opco_id,
                intermediate_s3,
                partitioned_files_key)

    else:
        future_table = get_active_and_future_tables("FUTURE", db_configs)
        future_table_name = future_table[0][0]

        future_table_query_result = check_table_is_empty(
            future_table_name, db_configs)
        if len(future_table_query_result) == 0:
            print('full load and future table empty, therefore load to future table ')
            # load future table
            db_configs['table'] = future_table_name
            load_data(
                db_configs,
                opco_id,
                intermediate_s3,
                partitioned_files_key)

            # update master db with future table effective date
            effective_date_result = get_effective_date(
                future_table_name, db_configs)
            update_table_effective_dates(
                db_configs, effective_date_result[0][0])
        else:
            # soft validation -> 1 -> we just print skip that opco return opco id metadata
            # hard validation -> 0 -> throw error
            # proceed -> 2 -> load irrespective of error
            if int(connection_params['soft_validation']) == 0:
                raise Exception("full load and future table is not empty")
            elif int(connection_params['soft_validation']) == 1:
                print(
                    'fill load and future table is not empty and soft validation hence job allowed to progress')
            elif int(connection_params['soft_validation']) == 2:
                print(
                    'load future table with full export even though '
                    'future table isnt empty and update effective date')
                db_configs['table'] = future_table_name
                load_data(
                    db_configs,
                    opco_id,
                    intermediate_s3,
                    partitioned_files_key)
            else:
                raise Exception("full load and future table is not empty")


def list_files_in_s3(bucket_name, prefix):
    """
               fetch partitioned files from s3
               """
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    matching_objects = []
    for page in pages:
        matching_objects.extend(page['Contents'])

    return matching_objects


class Configuration:
    """
         configuration class for constants
        """
    DOT = "."

    TABLE_NAME = "PRICE_ZONE_01"
    # opco_id substring depends on the column naming at spark job
    OUTPUT_PATH_PREFIX = "/opco_id="
    DATABASE_PREFIX = "REF_PRICE_"


def __create_db_engine(credentials):
    connect_url = sqlalchemy.engine.url.URL(
        'mysql+pymysql',
        username=credentials['username'],
        password=credentials['password'],
        host=credentials['host'],
        port=credentials['port'],
        database=credentials['database'])
    return sqlalchemy.create_engine(
        connect_url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30)


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              ['opco_id',
                               'cluster',
                               'partitioned_files_key',
                               'etl_timestamp',
                               'partial_load',
                               'ENV',
                               'intermediate_s3_name',
                               'intermediate_directory_path',
                               'METADATA_LAMBDA'])
    opco_id = args['opco_id']  # opco_id validation
    partitioned_files_key = args['partitioned_files_key']
    intermediate_s3 = args['intermediate_s3_name']
    data_arrival_timestamp = args['etl_timestamp']
    metadata_lambda = args['METADATA_LAMBDA']
    intermediate_directory_path = args['intermediate_directory_path']
    partial_load = args['partial_load']
    environment = args['ENV']
    cluster_id = args['cluster']
    etl_timestamp = args['etl_timestamp']

    print("Started data loading job for Opco: %s, file path: %s/%s\n" %
          (opco_id, intermediate_s3, partitioned_files_key))

    PARTIAL_LOAD_BOOL = str_to_bool(partial_load)

    try:
        find_tables_to_load(
            PARTIAL_LOAD_BOOL,
            environment,
            opco_id,
            intermediate_s3,
            partitioned_files_key)
    except Exception as e:
        raise Exception()
