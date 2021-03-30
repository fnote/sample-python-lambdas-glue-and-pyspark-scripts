import boto3
import pymysql
from datetime import datetime

ENV_PARAM_NAME = 'ENV'
CLUSTER_PARAM_NAME = 'cluster'
CLUSTER_N_OPCO_PARAM_NAME = 'cluster_opcos'
FILE_NAME_PARAM_NAME = 's3_object_key'
ETL_TIMESTAMP_PARAM_NAME = 'etl_timestamp'
ALLOCATED_JOB_COUNT_PARAM_NAME = 'allocated_job_count'
JOB_EXECUTION_STATUS_FETCH_QUERY = 'SELECT * FROM PRICE_ZONE_LOAD_JOB_EXECUTION_STATUS WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={} FOR UPDATE'
JOB_EXECUTION_STATUS_UPDATE_QUERY = 'UPDATE PRICE_ZONE_LOAD_JOB_EXECUTION_STATUS SET SUCCESSFUL_BUSINESS_UNITS = {}, FAILED_BUSINESS_UNITS = {}, FAILED_OPCO_IDS ="{}" , SUCCESSFUL_OPCO_IDS ="{}", END_TIME = {}, STATUS ="{}" WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={}'
CLUSTER_LOAD_JOB_COUNT_FETCH_QUERY = 'SELECT RUNNING_LOAD_JOB_COUNT FROM PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS WHERE CLUSTER_ID = {} FOR UPDATE'
CLUSTER_LOAD_JOB_COUNT_UPDATE_QUERY = 'UPDATE PRICE_ZONE_CLUSTER_LOAD_JOB_SETTINGS SET RUNNING_LOAD_JOB_COUNT = RUNNING_LOAD_JOB_COUNT - {} WHERE CLUSTER_ID  = {}'
TOTAL_BUSINESS_UNITS_COLUMN_NAME = 'TOTAL_BUSINESS_UNITS'
SUCCESSFUL_BUSINESS_UNITS_COLUMN_NAME = 'SUCCESSFUL_BUSINESS_UNITS'
FAILED_BUSINESS_UNITS_COLUMN_NAME = 'FAILED_BUSINESS_UNITS'
CLUSTER_LOAD_JOB_STATUSES_PARAM_NAME = 'loadJobStatuses'

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


def get_job_count_by_status(job_statuses, cluster_opco_count ,successful_opco_list ):
    success_count = 0
    print('load')
    for job_status in job_statuses:
        if 'loadJob' in job_status:
            if 'JobRunState' in job_status['loadJob']:
                opco_id = job_status['id']
                if job_status['loadJob']['JobRunState'] == 'SUCCEEDED':
                    success_count = success_count + 1
                    successful_opco_list.append(opco_id)

    return {'success_count': success_count, 'failure_count': cluster_opco_count - success_count , 'successful_opco_list': successful_opco_list}

def lambda_handler(event, context):
    #read file type also from here
    env = event[ENV_PARAM_NAME]
    cluster = event[CLUSTER_PARAM_NAME]
    file_name = event[FILE_NAME_PARAM_NAME]
    etl_timestamp = event[ETL_TIMESTAMP_PARAM_NAME]
    allocated_job_count = event[ALLOCATED_JOB_COUNT_PARAM_NAME]
    load_job_statuses = event[CLUSTER_LOAD_JOB_STATUSES_PARAM_NAME]
    cluster_opcos = event[CLUSTER_N_OPCO_PARAM_NAME.format(cluster)]
    cluster_opco_count = len(cluster_opcos)
    database_connection = get_db_connection(env)

    successful_opco_list = []
    job_statuses = get_job_count_by_status(load_job_statuses, cluster_opco_count, successful_opco_list)
    print(job_statuses)
    success_job_count = job_statuses['success_count']
    failed_job_count = job_statuses['failure_count']
    successful_opcos = job_statuses['successful_opco_list']

    failed_opco_list = list(set(cluster_opcos) - set(successful_opcos))

    should_backup = True

    try:
        cursor_object = database_connection.cursor()
        cursor_object.execute(JOB_EXECUTION_STATUS_FETCH_QUERY.format(file_name, etl_timestamp))
        result = cursor_object.fetchone()

        total_opco_count = int(result[TOTAL_BUSINESS_UNITS_COLUMN_NAME])
        successful_opco_count = int(result[SUCCESSFUL_BUSINESS_UNITS_COLUMN_NAME])
        failed_opco_count = int(result[FAILED_BUSINESS_UNITS_COLUMN_NAME])

        print('failed_opco_count ' + str(failed_opco_count))
        print('successful_opco_count ' + str(successful_opco_count))
        print('success_job_count ' + str(success_job_count))
        print('total_opco_count ' + str(total_opco_count))
        print('cluster_opco_count ' + str(cluster_opco_count))
        print('successful_opco_count ' + str(successful_opco_count))
        print('failed opco list'+ str(failed_opco_list))
        print('successful opco list ' + str(successful_opcos))

        # to make sure only one cluster does the backing  up
        if failed_opco_count > 0 or successful_opco_count + cluster_opco_count != total_opco_count:
            should_backup = False

        # read success load job count from input and write to DB
        # add file type , success opco ids failed opco ids status record count start time and end time
        successful_opcos_joined_string = ",".join(successful_opcos)
        failed_opcos_joined_string =  ",".join(failed_opco_list)
        status = "COMPLETED"
        date_time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor_object.execute(JOB_EXECUTION_STATUS_UPDATE_QUERY.format(successful_opco_count + success_job_count,
                                                                       failed_opco_count + failed_job_count, failed_opcos_joined_string, successful_opcos_joined_string, date_time_now, status, file_name,
                                                                       etl_timestamp))

        # fetch the load job count
        cursor_object.execute(CLUSTER_LOAD_JOB_COUNT_FETCH_QUERY.format(cluster))

        # update the load job count
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

successful_opco_list = []
failed_opco_list = []

test = [{
      "cluster": "1",
      "etl_timestamp": "1616863285",
      "partial_load": "False",
      "intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
      "intermediate_directory_path": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35",
      "id": "120",
      "partitioned_files_key": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35/partitioned",
      "ENV": "EXE",
      "loadJob": {
        "AllocatedCapacity": 1,
        "Arguments": {
          "--etl_timestamp": "1616863285",
          "--ENV": "EXE",
          "--intermediate_directory_path": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35",
          "--partitioned_files_key": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35/partitioned",
          "--opco_id": "120",
          "--partial_load": "False",
          "--intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
          "--cluster": "1"
        },
        "Attempt": 0,
        "CompletedOn": 1616890026649,
        "ExecutionTime": 1839,
        "GlueVersion": "1.0",
        "Id": "jr_f71101a9dddcddc57f6aecf9f97c5ce4b89b622ef1eacb1881282c0896bb62e5",
        "JobName": "CP-REF-etl-price-zone-load-job-EXE",
        "JobRunState": "SUCCEEDED",
        "LastModifiedOn": 1616890026649,
        "LogGroupName": "/aws-glue/python-jobs",
        "MaxCapacity": 1,
        "PredecessorRuns": [],
        "StartedOn": 1616888165424,
        "Timeout": 2880
      }
    },
    {
      "cluster": "1",
      "etl_timestamp": "1616863285",
      "partial_load": "False",
      "intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
      "intermediate_directory_path": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35",
      "id": "121",
      "partitioned_files_key": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35/partitioned",
      "ENV": "EXE",
      "loadJob": {
        "AllocatedCapacity": 1,
        "Arguments": {
          "--etl_timestamp": "1616863285",
          "--ENV": "EXE",
          "--intermediate_directory_path": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35",
          "--partitioned_files_key": "price_zone/all/etl_output_1616863285_92d30b0a-ba1d-4ccb-95cf-185c1c6fae35/partitioned",
          "--opco_id": "121",
          "--partial_load": "False",
          "--intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
          "--cluster": "1"
        },
        "Attempt": 0,
        "CompletedOn": 1616890029210,
        "ExecutionTime": 1840,
        "GlueVersion": "1.0",
        "Id": "jr_4b695d2d234670f4d529cb3382906193827b4b02970346b6387dc8f562f4ad4f",
        "JobName": "CP-REF-etl-price-zone-load-job-EXE",
        "JobRunState": "SUCCEEDED",
        "LastModifiedOn": 1616890029210,
        "LogGroupName": "/aws-glue/python-jobs",
        "MaxCapacity": 1,
        "PredecessorRuns": [],
        "StartedOn": 1616888165440,
        "Timeout": 2880
      }
    }];

# k = get_job_count_by_status(test, 3 ,successful_opco_list)
# print(k)

input = {
  "s3_path": "s3://cp-ref-etl-price-zone-storage-exe/ref_price_eats_60_opcos_final.csv.gz",
  "intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
  "partitioned_files_path": "s3://cp-ref-etl-output-bucket-exe/price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/partitioned/",
  "decompressed_file_path": "s3://cp-ref-etl-output-bucket-exe/price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/decompress.csv",
  "partitioned_files_key": "price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/partitioned",
  "etl_timestamp": "1616962868",
  "etl_output_path_key": "new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb",
  "s3_input_bucket": "cp-ref-etl-price-zone-storage-exe",
  "s3_input_file_key": "ref_price_eats_60_opcos_final.csv.gz",
  "partial_load": "True",
  "worker_count": 5,
  "worker_type": "G.2X",
  "active_opcos": "019,011,038,058,068,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179",
  "backup_bucket": "cp-ref-etl-data-backup-storage-exe",
  "backup_file_path": "price_zone/2021/March/28/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/",
  "intermediate_directory_path": "price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb",
  "ENV": "EXE",
  "file_type": "gz",
  "waitStatus": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "shouldWait": "false"
    },
    "SdkHttpMetadata": {
      "AllHttpHeaders": {
        "X-Amz-Executed-Version": [
          "$LATEST"
        ],
        "x-amzn-Remapped-Content-Length": [
          "0"
        ],
        "Connection": [
          "keep-alive"
        ],
        "x-amzn-RequestId": [
          "67123b8c-b542-4c60-bc56-8dacd39ce1ec"
        ],
        "Content-Length": [
          "21"
        ],
        "Date": [
          "Sun, 28 Mar 2021 20:21:12 GMT"
        ],
        "X-Amzn-Trace-Id": [
          "root=1-6060e537-7c1fe1cb36c82cd638d88d91;sampled=0"
        ],
        "Content-Type": [
          "application/json"
        ]
      },
      "HttpHeaders": {
        "Connection": "keep-alive",
        "Content-Length": "21",
        "Content-Type": "application/json",
        "Date": "Sun, 28 Mar 2021 20:21:12 GMT",
        "X-Amz-Executed-Version": "$LATEST",
        "x-amzn-Remapped-Content-Length": "0",
        "x-amzn-RequestId": "67123b8c-b542-4c60-bc56-8dacd39ce1ec",
        "X-Amzn-Trace-Id": "root=1-6060e537-7c1fe1cb36c82cd638d88d91;sampled=0"
      },
      "HttpStatusCode": 200
    },
    "SdkResponseMetadata": {
      "RequestId": "67123b8c-b542-4c60-bc56-8dacd39ce1ec"
    },
    "StatusCode": 200
  },
  "response": {
    "AllocatedCapacity": 10,
    "Arguments": {
      "--intermediate_directory_path": "price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb",
      "--partitioned_files_path": "s3://cp-ref-etl-output-bucket-exe/price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/partitioned/",
      "--active_opcos": "019,011,038,058,068,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179",
      "--enable-continuous-cloudwatch-log": "true",
      "--decompressed_file_path": "s3://cp-ref-etl-output-bucket-exe/price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/decompress.csv",
      "--file_type": "gz",
      "--s3_path": "s3://cp-ref-etl-price-zone-storage-exe/ref_price_eats_60_opcos_final.csv.gz",
      "--intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
      "--metadata_aggregator": "CP-REF-PRICE-ETL-metadata-aggregator-EXE"
    },
    "Attempt": 0,
    "CompletedOn": 1616964482760,
    "ExecutionTime": 1061,
    "GlueVersion": "2.0",
    "Id": "jr_b6f0c9120fe11916b45be5aa1bf2f16bf75a999ac0075a921e10b02530284ccf",
    "JobName": "CP-REF-etl-price-zone-transform-job-EXE",
    "JobRunState": "SUCCEEDED",
    "LastModifiedOn": 1616964482760,
    "LogGroupName": "/aws-glue/jobs",
    "MaxCapacity": 10,
    "NumberOfWorkers": 5,
    "PredecessorRuns": [],
    "StartedOn": 1616963414387,
    "Timeout": 2880,
    "WorkerType": "G.2X"
  },
  "opcoList": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "cluster_a": [
        "120"
      ],
      "cluster_b": [],
      "invalid_or_inactive_opco_list": []
    },
    "SdkHttpMetadata": {
      "AllHttpHeaders": {
        "X-Amz-Executed-Version": [
          "$LATEST"
        ],
        "x-amzn-Remapped-Content-Length": [
          "0"
        ],
        "Connection": [
          "keep-alive"
        ],
        "x-amzn-RequestId": [
          "56095230-dfa1-452f-acc7-5fe12e2549c6"
        ],
        "Content-Length": [
          "76"
        ],
        "Date": [
          "Sun, 28 Mar 2021 20:51:11 GMT"
        ],
        "X-Amzn-Trace-Id": [
          "root=1-6060ec3b-3ebd41d113d10d980aa2e09d;sampled=0"
        ],
        "Content-Type": [
          "application/json"
        ]
      },
      "HttpHeaders": {
        "Connection": "keep-alive",
        "Content-Length": "76",
        "Content-Type": "application/json",
        "Date": "Sun, 28 Mar 2021 20:51:11 GMT",
        "X-Amz-Executed-Version": "$LATEST",
        "x-amzn-Remapped-Content-Length": "0",
        "x-amzn-RequestId": "56095230-dfa1-452f-acc7-5fe12e2549c6",
        "X-Amzn-Trace-Id": "root=1-6060ec3b-3ebd41d113d10d980aa2e09d;sampled=0"
      },
      "HttpStatusCode": 200
    },
    "SdkResponseMetadata": {
      "RequestId": "56095230-dfa1-452f-acc7-5fe12e2549c6"
    },
    "StatusCode": 200
  },
  "nextStep": {
    "ExecutedVersion": "$LATEST",
    "Payload": {
      "nextStep": "Load All",
      "allocatedJobCount": 1
    },
    "SdkHttpMetadata": {
      "AllHttpHeaders": {
        "X-Amz-Executed-Version": [
          "$LATEST"
        ],
        "x-amzn-Remapped-Content-Length": [
          "0"
        ],
        "Connection": [
          "keep-alive"
        ],
        "x-amzn-RequestId": [
          "123a3fd9-9323-43fd-888d-a9a5d2453913"
        ],
        "Content-Length": [
          "48"
        ],
        "Date": [
          "Sun, 28 Mar 2021 20:51:13 GMT"
        ],
        "X-Amzn-Trace-Id": [
          "root=1-6060ec3f-19cef9f468c269c027e79a1c;sampled=0"
        ],
        "Content-Type": [
          "application/json"
        ]
      },
      "HttpHeaders": {
        "Connection": "keep-alive",
        "Content-Length": "48",
        "Content-Type": "application/json",
        "Date": "Sun, 28 Mar 2021 20:51:13 GMT",
        "X-Amz-Executed-Version": "$LATEST",
        "x-amzn-Remapped-Content-Length": "0",
        "x-amzn-RequestId": "123a3fd9-9323-43fd-888d-a9a5d2453913",
        "X-Amzn-Trace-Id": "root=1-6060ec3f-19cef9f468c269c027e79a1c;sampled=0"
      },
      "HttpStatusCode": 200
    },
    "SdkResponseMetadata": {
      "RequestId": "123a3fd9-9323-43fd-888d-a9a5d2453913"
    },
    "StatusCode": 200
  },
  "loadJobsResult": [
    {
      "cluster": "1",
      "etl_timestamp": "1616962868",
      "partial_load": "True",
      "intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
      "intermediate_directory_path": "price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb",
      "id": "120",
      "partitioned_files_key": "price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/partitioned",
      "ENV": "EXE",
      "loadJob": {
        "AllocatedCapacity": 1,
        "Arguments": {
          "--etl_timestamp": "1616962868",
          "--ENV": "EXE",
          "--intermediate_directory_path": "price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb",
          "--partitioned_files_key": "price_zone/new/etl_output_1616962868_91cf2a76-d0fa-437e-a41e-cfb2f5a3cdeb/partitioned",
          "--opco_id": "120",
          "--partial_load": "True",
          "--intermediate_s3_name": "cp-ref-etl-output-bucket-exe",
          "--cluster": "1"
        },
        "Attempt": 0,
        "CompletedOn": 1616965911958,
        "ExecutionTime": 1215,
        "GlueVersion": "1.0",
        "Id": "jr_7a731d681d9e5eb5e6fc89856dd4c0883d6fa5c2cea0af535c55d009d8682463",
        "JobName": "CP-REF-etl-price-zone-load-job-EXE",
        "JobRunState": "SUCCEEDED",
        "LastModifiedOn": 1616965911958,
        "LogGroupName": "/aws-glue/python-jobs",
        "MaxCapacity": 1,
        "PredecessorRuns": [],
        "StartedOn": 1616964674267,
        "Timeout": 2880
      }
    }
  ]
}

# g = lambda_handler(input,{})
# print(g)