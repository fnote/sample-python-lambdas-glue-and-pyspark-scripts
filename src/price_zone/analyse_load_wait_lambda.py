import boto3
import re
import MySQLdb
import logging
import os
from collections import Counter

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
from pandas import DataFrame

def extract_opco_id(x):
     p = re.search('opco_id=(\d+?)/', x['Key'])
     return p and p.group(1)

def load_cluster_details_from_common_db():
    # Open database connection
    db = MySQLdb.connect("cpe04dbcluster.cluster-ro-cvxmi1m5icyf.us-east-1.rds.amazonaws.com", "svccp000", "Cpaws000", "CPDB_Pricing01_019")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database.
    sql = "SELECT SUPC,CUSTOMER_ID,EXPORTED_DATE,END_DATE,PRICE from EXCEPTION"
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()


    except:
        print("Error: unable to fetch data")

    # disconnect from server
    db.close()

def get_counts(attr):
    db = MySQLdb.connect("cp-ref-price-db-cluster-01-dev.cluster-c6xai0tt38eb.us-east-1.rds.amazonaws.com", "cpuser", "a4T6XrIXJGwp",
                         "COMMON_DB")
    print(attr)
    sql = "SELECT Value from settings WHERE setting = ?" ,attr

    cursor = db.cursor()
    try:
        # Execute the SQL command
        cursor.execute("SELECT Value from settings WHERE setting = '%s'" %(attr))
        # Fetch all the rows in a list of lists.
        results = cursor.fetchone()
        print(results)
        print(results[0][0])
        return results[0][0]


    except:
        print("Error: unable to fetch data")

def lambda_handler(event, context):
    logger.info("Received event:")
    logger.info(event)
    env = os.environ['env']

    max_load_jobs_key = 'MAX_REF_LOAD_JOB_COUNT_' + env
    running_load_jobs_key = 'RUNNING_REF_LOAD_JOB_COUNT_' + env

    # cluster_a = event['cluster_a']
    cluster_a = [0]

    if len(cluster_a) == 0:
        return {
                'nextStep': 'Terminate'
            }
        #next step terminate
    else:
        #call common db
        max_load_jobs = get_counts('MAX_REF_LOAD_JOB_COUNT')
        currently_running_load_jobs = get_counts('RUNNING_REF_LOAD_JOB_COUNT')
        print(max_load_jobs, currently_running_load_jobs)

        if currently_running_load_jobs < max_load_jobs:
            print('max load jobs not reached yet, proceed to the load step')
            return {
                'nextStep': 'load'
            }
        else:
            print('max load jobs not reached , therefore moved to the wait state')
            return {
                'nextStep': 'wait'
            }

        #next step load

lambda_handler([],[])
    # If cluster_a is empty (cluster_a : []); return { nextStep: 'terminate' }
    #
    # if cluster_a is non empty query the common db and identify the max allowed load job count and running load job count
    #
    #     if running count < max  allowed count then do the load job count update and return { nextStep: 'load' }
    #
    #     if running count = max allowed count then return { nextStep: 'wait' }