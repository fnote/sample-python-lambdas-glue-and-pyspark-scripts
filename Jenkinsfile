import groovy.json.*

def bucket = 'sysco-us-east-1-prcp-nonprod-codedeploy'
def pathPrefix = 'ReferencePricingApi'
def pathSuffix
def ENV
def s3Path
def s3key
def region =  'us-east-1'

def updateGlueScript(region, jobName, scriptLocation) {

    echo "Deploying Glue Job ${jobName}"
    def jobJson = bat (
            script: "@aws glue get-job --job-name ${jobName} --region ${region}",
            returnStdout: true
    ).trim()

    def json = new JsonSlurperClassic().parseText(jobJson)
    def jobParams = json.Job
    jobParams.remove('Name')
    jobParams.remove('CreatedOn')
    jobParams.remove('LastModifiedOn')
    jobParams.remove('AllocatedCapacity')
    if (jobParams.NumberOfWorkers) {
        jobParams.remove('MaxCapacity')
    }
    jobParams.Command.ScriptLocation = scriptLocation

    def escapedStr = StringEscapeUtils.escapeJava(JsonOutput.toJson(jobParams))
    echo "Escpated Str: ${escapedStr}"

    def output = bat (
            script: "aws glue update-job \
        --job-name ${jobName} \
        --job-update \"${escapedStr}\" \
        --region ${region}",
            returnStdout: true
    ).trim()
    echo "Output: ${output}"
}

def updateGlueScriptProd(region, jobName, scriptLocation) {
    echo "Deploying Glue Job ${jobName}"
    def jobJson = bat (
            script:  "@aws sts assume-role --role-arn \"arn:aws:iam::130227353653:role/PRCP-Jenkins-CodeDeploy-Role\" --role-session-name \"Jenkins-CD-Session\">temCredentials.json\n" +
                    "for /f %%i in ('jq -r .Credentials.AccessKeyId temCredentials.json') do SET AWS_ACCESS_KEY_ID=%%i\n" +
                    "for /f %%j in ('jq -r .Credentials.SecretAccessKey temCredentials.json') do SET AWS_SECRET_ACCESS_KEY=%%j\n" +
                    "for /f %%k in ('jq -r .Credentials.SessionToken temCredentials.json') do SET AWS_SESSION_TOKEN=%%k\n" +
                    "aws glue get-job --job-name ${jobName} --region ${region} > jobOutput.json",
            returnStdout: true
    ).trim()

    def pwd = bat (
            script: "@chdir",
            returnStdout: true
    ).trim()

    def jobOutputFileName = "${pwd}\\jobOutput.json"
    File jobOutputFile = new File(jobOutputFileName)
    def json = new JsonSlurperClassic().parse(jobOutputFile, 'utf-8')
    def jobParams = json.Job
    jobParams.remove('Name')
    jobParams.remove('CreatedOn')
    jobParams.remove('LastModifiedOn')
    jobParams.remove('AllocatedCapacity')
    if (jobParams.NumberOfWorkers) {
        jobParams.remove('MaxCapacity')
    }
    jobParams.Command.ScriptLocation = scriptLocation

    def escapedStr = StringEscapeUtils.escapeJava(JsonOutput.toJson(jobParams))
    echo "Escpated Str: ${escapedStr}"

    def output = bat (
            script: "aws sts assume-role --role-arn \"arn:aws:iam::130227353653:role/PRCP-Jenkins-CodeDeploy-Role\" --role-session-name \"Jenkins-CD-Session\">temCredentials.json\n" +
                    "for /f %%i in ('jq -r .Credentials.AccessKeyId temCredentials.json') do SET AWS_ACCESS_KEY_ID=%%i\n" +
                    "for /f %%j in ('jq -r .Credentials.SecretAccessKey temCredentials.json') do SET AWS_SECRET_ACCESS_KEY=%%j\n" +
                    "for /f %%k in ('jq -r .Credentials.SessionToken temCredentials.json') do SET AWS_SESSION_TOKEN=%%k\n" +
                    "aws glue update-job \
        --job-name ${jobName} \
        --job-update \"${escapedStr}\" \
        --region ${region}",
            returnStdout: true
    ).trim()
    echo "Output: ${output}"
}

def updateLambda(bucket, region, fnName, scriptLocationKey) {
    echo "Deploying Lambda ${fnName}"
    def output = bat (
            script: "aws lambda update-function-code --function-name ${fnName} \
        --s3-bucket ${bucket} \
        --s3-key ${scriptLocationKey} \
        --region ${region}",
            returnStdout: true
    ).trim()
    echo "Output: ${output}"
}

def updateLambdaProd(bucket, region, fnName, scriptLocationKey) {
    echo "Deploying Lambda ${fnName}"
    def output = bat (
            script: "aws sts assume-role --role-arn \"arn:aws:iam::130227353653:role/PRCP-Jenkins-CodeDeploy-Role\" --role-session-name \"Jenkins-CD-Session\">temCredentials.json\n" +
                    "for /f %%i in ('jq -r .Credentials.AccessKeyId temCredentials.json') do SET AWS_ACCESS_KEY_ID=%%i\n" +
                    "for /f %%j in ('jq -r .Credentials.SecretAccessKey temCredentials.json') do SET AWS_SECRET_ACCESS_KEY=%%j\n" +
                    "for /f %%k in ('jq -r .Credentials.SessionToken temCredentials.json') do SET AWS_SESSION_TOKEN=%%k\n" +
                    "aws lambda update-function-code --function-name ${fnName} \
        --s3-bucket ${bucket} \
        --s3-key ${scriptLocationKey} \
        --region ${region}",
            returnStdout: true
    ).trim()
    echo "Output: ${output}"
}

def zipScript(dir, name, zipDirectory = false) {
    def recursive = ''
    def scriptName = ''
    if (zipDirectory) {
        recursive = '-r '
    } else {
        scriptName = "${name} "
    }
    bat script: "cd ${dir} & D:/winrar/winrar a ${recursive}${name}.zip ${scriptName}& dir"
}

def copyToS3(sourceFile, destinationPath) {
    bat script: "aws s3 cp ${sourceFile} ${destinationPath}/"
}

def copyFileIntoEnv(s3Path) {
    def paS3Path = "${s3Path}/pa"
    copyToS3("./src/price_zone/s3_trigger_lambda.py.zip", s3Path)
    copyToS3("./src/price_zone/analyze_etl_wait_status.py.zip", s3Path)
    copyToS3("./src/price_zone/decompress_job.py", s3Path)
    copyToS3("./src/price_zone/transform_spark_job.py", s3Path)
    copyToS3("./src/price_zone/load_job.py", s3Path)
    copyToS3("./src/price_zone/data_backup_job.py", s3Path)

//    PA
    copyToS3("./src/pa/s3_trigger_lambda.py.zip", paS3Path)
    copyToS3("./src/pa/pa_etl_script.py", paS3Path)
    copyToS3("./src/pa/data_backup_job.py", paS3Path)

//    Common
    copyToS3("./src/Notifier/Notifier.zip", s3Path)
    copyToS3("./src/common/metadata_aggregator.py.zip", s3Path)
}

def deployIntoEnv(env, bucket, s3Path, s3key, region) {
    updateLambda(
            bucket, region, "CP-REF-etl-price-zone-trigger-${env}",
            "${s3key}/s3_trigger_lambda.py.zip")
    updateLambda(
            bucket, region, "CP-REF-etl-price-zone-wait-status-analyzer-${env}",
            "${s3key}/analyze_etl_wait_status.py.zip")
    updateGlueScript(
            region, "CP-REF-etl-prize-zone-decompression-job-${env}",
            "${s3Path}/decompress_job.py")
    updateGlueScript(
            region, "CP-REF-etl-prize-zone-transform-job-${env}",
            "${s3Path}/transform_spark_job.py")
    updateGlueScript(
            region, "CP-REF-etl-prize-zone-load-job-${env}",
            "${s3Path}/load_job.py")
    updateGlueScript(
            region, "CP-REF-etl-prize-zone-backup-job-${env}",
            "${s3Path}/data_backup_job.py")

//    PA
    updateLambda(
            bucket, region, "CP-REF-etl-pa-trigger-${env}",
            "${s3key}/pa/s3_trigger_lambda.py.zip")
    updateGlueScript(
            region, "CP-REF-etl-pa-job-${env}",
            "${s3Path}/pa/pa_etl_script.py")
    updateGlueScript(
            region, "CP-REF-etl-pa-backup-job-${env}",
            "${s3Path}/pa/data_backup_job.py")

//    Common
    updateLambda(
            bucket, region, "CP-REF-etl-notifier-${env}",
            "${s3key}/Notifier.zip")
    updateLambda(
            bucket, region, "CP-REF-PRICE-etl-metadata-aggregator-${env}",
            "${s3key}/metadata_aggregator.py.zip")
}

pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                script {
                    zipScript("src/price_zone", "s3_trigger_lambda.py")
                    zipScript("src/price_zone", "analyze_etl_wait_status.py")
                    bat script: "cd src/Notifier & pip3 install --target . -r requirements.txt & D:/winrar/winrar a -r Notifier.zip & dir"
                    zipScript("src/pa/", "s3_trigger_lambda.py")
                    zipScript("src/common/", "metadata_aggregator.py")
                }
            }
        }

        stage('Prepare') {
            steps {
                script {
                    def commit =  bat (
                            script: "@git log -n 1 --pretty=format:'%%h'",
                            returnStdout: true
                    ).trim().replaceAll (/'/, '')

                    def now = new Date()
                    def date = now.format("YYY-MM-dd", TimeZone.getTimeZone('UTC'))
                    pathSuffix = "${date}/${commit}"
                }
            }
        }

        stage('DEV: Approval') {
            options {
                timeout(time: 6, unit: 'HOURS')
            }
            steps {
                script {
                    input id: 'Deploy', message: 'Do you want to deploy to Dev Environment?', submitter: 'admin'
                    ENV = 'DEV'
                    s3key = "${pathPrefix}/${ENV}/${pathSuffix}"
                    s3Path = "s3://${bucket}/${s3key}"
                    echo "Files will be to ${s3Path}"
                }
            }
        }

        stage('DEV: Copy') {
            steps {
                script {
                    copyFileIntoEnv(s3Path)
                }
            }
        }

        stage('DEV: Deploy') {
            steps {
                script {
                    deployIntoEnv(ENV, bucket, s3Path, s3key, region)
                }
            }
        }

        stage('EXE: Approval') {
            options {
                timeout(time: 6, unit: 'HOURS')
            }
            steps {
                script {
                    input id: 'Deploy', message: 'Do you want to deploy to EXE Environment?', submitter: 'admin'
                    ENV = 'EXE'
                    s3key = "${pathPrefix}/${ENV}/${pathSuffix}"
                    s3Path = "s3://${bucket}/${s3key}"
                    echo "Files will be to ${s3Path}"
                }
            }
        }

        stage('EXE: Copy') {
            steps {
                script {
                    copyFileIntoEnv(s3Path)
                }
            }
        }

        stage('EXE: Deploy') {
            steps {
                script {
                    deployIntoEnv(ENV, bucket, s3Path, s3key, region)
                }
            }
        }

        stage('STG: Approval') {
            options {
                timeout(time: 6, unit: 'HOURS')
            }
            steps {
                script {
                    input id: 'Deploy', message: 'Do you want to deploy to STG Environment?', submitter: 'admin'
                    ENV = 'STG'
                    s3key = "${pathPrefix}/${ENV}/${pathSuffix}"
                    s3Path = "s3://${bucket}/${s3key}"
                    echo "Files will be to ${s3Path}"
                }
            }
        }

        stage('STG: Copy') {
            steps {
                script {
                    copyFileIntoEnv(s3Path)
                }
            }
        }

        stage('STG: Deploy') {
            steps {
                script {
                    deployIntoEnv(ENV, bucket, s3Path, s3key, region)
                }
            }
        }

        stage('PROD: Approval') {
            options {
                timeout(time: 6, unit: 'HOURS')
            }
            steps {
                script {
                    input id: 'Deploy', message: 'Do you want to deploy to PROD Environment?', submitter: 'admin'
                    ENV = 'PROD'
                    s3key = "${pathPrefix}/${ENV}/${pathSuffix}"
                    s3Path = "s3://${bucket}/${s3key}"
                    echo "Files will be to ${s3Path}"
                }
            }
        }

        stage('PROD: Copy') {
            steps {
                script {
                    copyFileIntoEnv(s3Path)
                }
            }
        }

        stage('PROD: Deploy') {
            steps {
                script {
                    updateLambdaProd(
                            bucket, region, "CP-REF-etl-price-zone-trigger-${ENV}",
                            "${s3key}/s3_trigger_lambda.py.zip")
                    updateLambdaProd(
                            bucket, region, "CP-REF-etl-price-zone-wait-status-analyzer-${ENV}",
                            "${s3key}/analyze_etl_wait_status.py.zip")
                    updateGlueScriptProd(
                            region, "CP-REF-etl-prize-zone-decompression-job-${ENV}",
                            "${s3Path}/decompress_job.py")
                    updateGlueScriptProd(
                            region, "CP-REF-etl-prize-zone-transform-job-${ENV}",
                            "${s3Path}/transform_spark_job.py")
                    updateGlueScriptProd(
                            region, "CP-REF-etl-prize-zone-load-job-${ENV}",
                            "${s3Path}/load_job.py")
                    updateGlueScriptProd(
                            region, "CP-REF-etl-prize-zone-backup-job-${ENV}",
                            "${s3Path}/data_backup_job.py")

                    //    PA
                    updateLambdaProd(
                            bucket, region, "CP-REF-etl-pa-trigger-${ENV}",
                            "${s3key}/pa/s3_trigger_lambda.py.zip")
                    updateGlueScriptProd(
                            region, "CP-REF-etl-pa-job-${ENV}",
                            "${s3Path}/pa/pa_etl_script.py")
                    updateGlueScriptProd(
                            region, "CP-REF-etl-pa-backup-job-${ENV}",
                            "${s3Path}/pa/data_backup_job.py")

                    //    Common
                    updateLambdaProd(
                            bucket, region, "CP-REF-etl-notifier-${ENV}",
                            "${s3key}/Notifier.zip")
                    updateLambdaProd(
                            bucket, region, "CP-REF-PRICE-etl-metadata-aggregator-${ENV}",
                            "${s3key}/metadata_aggregator.py.zip")
                }
            }
        }
    }
}
