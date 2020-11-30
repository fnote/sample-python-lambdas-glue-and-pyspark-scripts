def bucket = 'sysco-us-east-1-prcp-nonprod-codedeploy'
def pathPrefix = 'DiscountService'
def pathSuffix
def ENV
def s3Path
def s3key
def region =  'us-east-1'

def updateGlueScript(region, jobName, role, scriptLocation, type) {
    echo "Deploying Glue Job ${jobName}"
    def output = bat (
            script: "aws glue update-job \
        --job-name ${jobName} \
        --job-update Role=${role},Command=\"{Name=${type},ScriptLocation=${scriptLocation}}\" \
        --region ${region}",
            returnStdout: true
    ).trim()
    echo "Output: ${output}"
}

def updateGlueScriptProd(region, jobName, role, scriptLocation, type) {
    echo "Deploying Glue Job ${jobName}"
    def output = bat (
            script: "aws sts assume-role --role-arn \"arn:aws:iam::130227353653:role/PRCP-Jenkins-CodeDeploy-Role\" --role-session-name \"Jenkins-CD-Session\">temCredentials.json\n" +
                    "for /f %%i in ('jq -r .Credentials.AccessKeyId temCredentials.json') do SET AWS_ACCESS_KEY_ID=%%i\n" +
                    "for /f %%j in ('jq -r .Credentials.SecretAccessKey temCredentials.json') do SET AWS_SECRET_ACCESS_KEY=%%j\n" +
                    "for /f %%k in ('jq -r .Credentials.SessionToken temCredentials.json') do SET AWS_SESSION_TOKEN=%%k\n" +
                    "aws glue update-job \
        --job-name ${jobName} \
        --job-update Role=${role},Command=\"{Name=${type},ScriptLocation=${scriptLocation}}\" \
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
    copyToS3("./src/Notifier/Notifier.zip", s3Path)
    copyToS3("./src/customer_eligibility.py", s3Path)
    copyToS3("./src/customer_eligibility_partitioner.py", s3Path)
    copyToS3("./src/data_backup_job.py", s3Path)
    copyToS3("./src/decompress_job.py", s3Path)
    copyToS3("./src/mdt.py", s3Path)
    copyToS3("./src/TriggerLambda.py.zip", s3Path)
}

def deployIntoEnv(env, bucket, s3Path, s3key, region) {
    updateLambda(
            bucket, region, "CP-REF-etl-notifier-${env}",
            "${s3key}/Notifier.zip")

    updateGlueScript(
            region, "CP-DISCOUNTS-etl-customer-eligibility-load-job-${env}",
            "CP-DISCOUNTS-ETLGlueRole-${env}",
            "${s3Path}/customer_eligibility.py", "pythonshell")

    updateGlueScript(
            region, "CP-DISCOUNTS-etl-customer-eligibility-partition-job-${env}",
            "CP-DISCOUNTS-ETLGlueRole-${env}",
            "${s3Path}/customer_eligibility_partitioner.py", "glueetl")

    updateGlueScript(
            region, "CP-DISCOUNTS-etl-customer-eligibility-partition-job-${env}",
            "CP-DISCOUNTS-ETLGlueRole-${env}",
            "${s3Path}/data_backup_job.py", "pythonshell")

    updateGlueScript(
            region, "CP-DISCOUNTS-etl-customer-eligibility-decompress-job-${env}",
            "CP-DISCOUNTS-ETLGlueRole-${env}",
            "${s3Path}/decompress_job.py", "pythonshell")

    updateGlueScript(
            region, "CP-DISCOUNTS-etl-mdt-load-job-${env}",
            "CP-DISCOUNTS-ETLGlueRole-${env}",
            "${s3Path}/mdt.py", "pythonshell")

    updateLambda(
            bucket, region, "CP-DISCOUNTS-etl-trigger-${env}",
            "${s3key}/TriggerLambda.py.zip")
}

pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                script {
                    zipScript("src/Notifier", "Notifier", true)
                    zipScript("src", "TriggerLambda.py")
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
                            bucket, region, "CP-REF-etl-notifier-${ENV}",
                            "${s3key}/Notifier.zip")

                    updateGlueScriptProd(
                            region, "CP-DISCOUNTS-etl-customer-eligibility-load-job-${ENV}",
                            "CP-DISCOUNTS-ETLGlueRole-${ENV}",
                            "${s3Path}/customer_eligibility.py", "pythonshell")

                    updateGlueScriptProd(
                            region, "CP-DISCOUNTS-etl-customer-eligibility-partition-job-${ENV}",
                            "CP-DISCOUNTS-ETLGlueRole-${ENV}",
                            "${s3Path}/customer_eligibility_partitioner.py", "glueetl")

                    updateGlueScriptProd(
                            region, "CP-DISCOUNTS-etl-customer-eligibility-partition-job-${ENV}",
                            "CP-DISCOUNTS-ETLGlueRole-${ENV}",
                            "${s3Path}/data_backup_job.py", "pythonshell")

                    updateGlueScriptProd(
                            region, "CP-DISCOUNTS-etl-customer-eligibility-decompress-job-${env}",
                            "CP-DISCOUNTS-ETLGlueRole-${ENV}",
                            "${s3Path}/decompress_job.py", "pythonshell")

                    updateGlueScriptProd(
                            region, "CP-DISCOUNTS-etl-mdt-load-job-${ENV}",
                            "CP-DISCOUNTS-ETLGlueRole-${ENV}",
                            "${s3Path}/mdt.py", "pythonshell")

                    updateLambdaProd(
                            bucket, region, "CP-DISCOUNTS-etl-trigger-${ENV}",
                            "${s3key}/TriggerLambda.py.zip")
                }
            }
        }
    }
}
