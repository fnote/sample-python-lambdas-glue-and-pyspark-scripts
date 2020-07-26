# reference-pricing-service-scripts
AWS Glue scripts which are used to load data to databases


#### Executing unit tests

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

**Note**: Makesure to install pyspark version compatible with spark version

##### Executing unit tests

`python -W ignore -m unittest discover -s test/  -v`

##### Test execution with coverage

##### **Prerequisite**

`pip install coverage`

##### Execute tests with _Coverage_

`coverage run --source src -m unittest discover -s src test`

##### Generate report

`coverage report -m`
