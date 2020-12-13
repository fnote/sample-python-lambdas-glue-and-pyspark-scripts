# reference-pricing-service-scripts
AWS Glue scripts which are used to load data to databases


## Executing unit tests

Unit testing pyspark scripts

##### Prerequisites

1. Install the Apache Spark distribution from one of the following locations:

    https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz 
or

    https://spark.apache.org/downloads.html

2.  Export the **SPARK_HOME** environment variable, setting it to the root location extracted from the Spark archive. For example:

    For Glue version 1.0: export SPARK_HOME=/home/$USER/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8

3.  Install pyspark
 
    `pip install pyspark`

4.  Install boto3
    
    `pip install boto3`

5.  Install mock   
    
    `pip install mock`

**Note**: Makesure to install pyspark version compatible with spark version

##### Executing unit tests

`python -W ignore -m unittest discover -s test/  -v`
or `python3 -W ignore -m unittest discover -s test/  -v`

If you receive any errors due to python version mismatches (Exception: Python in worker has different version 2.7 than that in driver 3.6) try doing the following

`export PYSPARK_PYTHON=the_python_location`

 `export PYSPARK_DRIVER_PYTHON=the_python_location` 

If you receive the error "Unsupported class file major version 55, you need export the path of Java 8 as JAVA_HOME

`export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home`

##### Test execution with coverage

##### **Prerequisite**

`pip install coverage`

##### Execute tests with _Coverage_

`coverage run --source src -m unittest discover -s src test`

##### Generate report

`coverage report -m`
