# S3toDDB
CSV file upload to a S3 bucket and store the data into target Collection and Archive DynamoDB tables

### Lambda function
* [lambda_function.py](lambda_function.py)

### Deploy VTDLP 3toDDB Lambda function using CloudFormation stack
#### Step 1: Launch CloudFormation stack
[![Launch Stack](https://cdn.rawgit.com/buildkite/cloudformation-launch-stack-button-svg/master/launch-stack.svg)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?&templateURL=https://vtdlp-dev-cf.s3.amazonaws.com/6baabe22b0a79b1c8849d8afe50eeb51.template)

Click *Next* to continue

#### Step 2: Specify stack details

| Name | Description |
|----------|:-------------:|
| Stack name | any valid name |
| APP_IMG_ROOT_PATH | https://img.cloud.lib.vt.edu/iawa/ |
| Bibliographic_Citation | Researchers ...... |
| Collection_Category | IAWA |
| DYNO_Collection_TABLE | collectiontablename |
| DYNO_Archive_TABLE | archivetablename |
| NOID_NAA | 53696 |
| NOID_Scheme | ark:/ |
| NOID_Template | eeddeede |
| REGION | us-east-1 |
| Rights_Holder | Special Collections, VTL |
| Rights_Statement | Permission ...... |

#### Step 3: Configure stack options
Leave it as is and click **Next**

#### Step 4: Review
Make sure all checkboxes under Capabilities section are **CHECKED**

Click *Create stack*

### Deploy VTDLP 3toDDB Lambda function using SAM CLI

To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* [Python 3 installed](https://www.python.org/downloads/)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

To build and deploy your application for the first time, run the following in your shell:

```bash
sam build --use-container
```

Above command will build the source of the application. The SAM CLI installs dependencies defined in `requirements.txt`, creates a deployment package, and saves it in the `.aws-sam/build` folder.

To package the application, run the following in your shell:
```bash
sam package --output-template-file packaged.yaml --s3-bucket BUCKETNAME
```
Above command will package the application and upload it to the S3 bucket you specified.

Run the following in your shell to deploy the application to AWS:
```bash
sam deploy --template-file packaged.yaml --stack-name STACKNAME --s3-bucket BUCKETNAME --parameter-overrides 'APPIMGROOTPATH=https://yourURL/ BibliographicCitation="Your sentance" CollectionCategory=collection type DYNOCollectionTABLE=CollectionTableName DYNOArchiveTABLE=ArchiveTableName NOIDNAA=53696 NOIDScheme=ark:/ NOIDTemplate=eeddeede REGION=us-east-1 RightsHolder="Your sentance" RightsStatement="Your sentance" S3BucketName=S3BucketName' --capabilities CAPABILITY_IAM --region us-east-1
```

### Usage
* Prepare "collection_metadata.csv" and "index.csv" for Collection and Item ingestion, respectively.
* Put "collection_metadata.csv" and "index.csv" to the target S3 bucket created after deployment.
* Create a test event in Lambda function. Set the S3 bucket name as in the previous step. For Collection ingestion, set the object key as "collection_metadata.csv"; for Item ingestion, set the object key as "index.csv."
* Go to DynamoDB to see the end results in Collection and Archive tables.

### Cleanup

To delete the sample application that you created, use the AWS CLI. Assuming you used your project name for the stack name, you can run the following:

```bash
aws cloudformation delete-stack --stack-name stackname
```