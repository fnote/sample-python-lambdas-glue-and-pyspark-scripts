import sys
import threading

import boto3
import base64

import sqlalchemy as sqlalchemy
from queue import Queue
from sqlalchemy.pool import QueuePool
from awsglue.utils import getResolvedOptions


def list_files_in_s3(bucketname, prefix):
    s3 = boto3.client('s3')
    all_objects = s3.list_objects_v2(Bucket=bucketname, Prefix=prefix)
    return all_objects


class Configuration:
    DOT = "."

    TABLE_NAME = "PRICE_ZONE_01"
    GLUE_CONNECTION_NAME = 'cp-ref-data-poc-common'
    OUTPUT_PATH_PREFIX = "/opco_id="  # opco_id substring depends on the column naming at spark job
    DATABASE_PREFIX = "REF_PRICE_"


def __create_db_engine(credentials):
    connect_url = sqlalchemy.engine.url.URL(
        'mysql+pymysql',
        username=credentials['username'],
        password=credentials['password'],
        host=credentials['host'],
        port=credentials['port'],
        database=credentials['database'])
    return sqlalchemy.create_engine(connect_url, poolclass=QueuePool, pool_size=5, max_overflow=10, pool_timeout=30)


def _execute_load(pool, queue, database, table, threadErrors):
    while not queue.empty():
        s3_file_path = queue.get()
        try:
            table_with_database_name = database + Configuration.DOT + table
            load_qry = "LOAD DATA FROM S3 '" + s3_file_path + "'" \
                                                              "REPLACE INTO TABLE " + table_with_database_name + \
                       " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' " \
                       "IGNORE 1 LINES (@supc,@price_zone,@customer_id,@effective_date) SET " \
                       "SUPC=@supc," \
                       "CUSTOMER_ID=@customer_id," \
                       "EFFECTIVE_DATE=@effective_date," \
                       "PRICE_ZONE=@price_zone;"

            connection = pool.connect()
            print("Populating price zone data from file: %s to table %s\n" % (s3_file_path, table_with_database_name))
            connection.execute(load_qry)
            print(
                "Completed loading price zone data from s3 %s to table %s\n" % (s3_file_path, table_with_database_name))
            connection.close()
        except Exception as e:
            print("Error occurred when trying to file: %s, error: %s" % (s3_file_path, repr(e)))
            threadErrors.append(repr(e))
            raise e


def load_data(dbconfigs, opco_id, bucketname, partitioned_files_path):
    prefix = partitioned_files_path + Configuration.OUTPUT_PATH_PREFIX + opco_id
    output_files = list_files_in_s3(bucketname, prefix)['Contents']
    zero_prefix = "0"
    if len(opco_id) == 2:
        opco_id = zero_prefix + opco_id
    dbconfigs['database'] = Configuration.DATABASE_PREFIX + opco_id
    pool = __create_db_engine(dbconfigs)
    queue = Queue()
    for file in output_files:
        prefix_path = prefix + '/'
        if file['Key'] != prefix_path:
            filename = file['Key'].replace(prefix_path, '')
            s3_file_path = "s3://" + bucketname + "/" + prefix_path + filename
            queue.put(s3_file_path)

    threadErrors = []
    threads = []
    for i in range(0, 4):
        threads.append(threading.Thread(target=_execute_load,
                                        args=(pool, queue, dbconfigs['database'], dbconfigs['table'], threadErrors)))

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print(threadErrors)

    if len(threadErrors) > 0:
        raise Exception(threadErrors)


def _retrieve_conection_details():
    glue = boto3.client('glue', region_name='us-east-1')

    response = glue.get_connection(Name=Configuration.GLUE_CONNECTION_NAME)

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
        'table': Configuration.TABLE_NAME
    }


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['opco_id', 'partitioned_files_key', 'intermediate_s3_name'])
    opco_id = args['opco_id']  # opco_id validation
    partitioned_files_key = args['partitioned_files_key']
    intermediate_s3 = args['intermediate_s3_name']
    print(
        "Started data loading job for Opco: %s, file path: %s/%s\n" % (opco_id, intermediate_s3, partitioned_files_key))
    dbconfigs = _retrieve_conection_details()

    load_data(dbconfigs, opco_id, intermediate_s3, partitioned_files_key)
