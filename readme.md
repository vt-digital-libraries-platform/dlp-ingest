# S3toDDB
CSV file upload to a S3 bucket and store the data into target Collection and Archive DynamoDB tables

This Lambda function is for metadata ingesetion for the [VTDLP Access Website](https://github.com/VTUL/dlp-access). It supports [collection](examples/collection_metadata.csv) and [archive(item)](examples/index.csv) metadata.  

Before you deploy this Lambda function, you should already have [VTDLP Access Website](https://github.com/VTUL/dlp-access) deployed ready, and `ID Minting service` and `Resolution service` deployed ready via [DLPservices](https://github.com/vt-digital-libraries-platform/DLPServices).

* The `DYNOCollectionTABLE`, `DYNOArchiveTABLE`, and `DYNOCollectionmapTABLE` table name information are from the DynamoDB after [VTDLP Access Website](https://github.com/VTUL/dlp-access) is deployed. See detailss [here](https://github.com/VTUL/dlp-access/blob/LIBTD-2489/docs/deploy.md#step-2-add-the-site-configuration-data)

* The `LongURLPath` should be your `VTDLP Access Website`'s URL. E.g. `https://xxxx.yyyy.amplifyapp.com/` or your custom domain name `https://iawa.lib.vt.edu/`. Note: The URL should end with a slash `/`.

* The `ShortURLPath` should be your `Resolution service`'s URL. E.g. `https://xxxx.execute-api.us-east-1.amazonaws.com/Prod/` or your custom domain name `http://idn.lib.vt.edu/`. The `Resolution service` API Gateway name should look something like `Resolution Service API-tablename`. Note: The URL should end with a slash `/`.

* The `APIKey` and `APIEndpoint` information are from the API gateway after [ID Minting service](https://github.com/vt-digital-libraries-platform/DLPServices) is deployed. The `ID Minting service` API Gateway name should look something like `Mint Service API-tablename`. Note: The `APIEndpoint` URL should end with a slash `/`.

* The `APPIMGROOTPATH` is a URL point to your Cloudfront URL which serves the static images. E.g. https://img.cloud.lib.vt.edu/iawa/. Note: The URL should end with a slash `/`.

You can use two different methods to deploy VTDLP Services. The first method is using CloudFormation stack and the second method is using SAM CLI.

### Deploy VTDLP S3toDDB Lambda function using CloudFormation stack
#### Step 1: Launch CloudFormation stack
[![Launch Stack](https://cdn.rawgit.com/buildkite/cloudformation-launch-stack-button-svg/master/launch-stack.svg)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?&templateURL=https://vtdlp-dev-cf.s3.amazonaws.com/d98032c5d142404a8e43e34faf0b74d5.template)

Click *Next* to continue

#### Step 2: Specify stack details

* <b>Stack name</b>: Stack name can include letters (A-Z and a-z), numbers (0-9), and dashes (-).

* <b>Parameters</b>: Parameters are defined in your template and allow you to input custom values when you create or update a stack.

    | Name | Description | Note |
    |----------|:-------------:|:-------------:|
    | APPIMGROOTPATH | Cloudfront URL which serves the static images. E.g. https://img.cloud.lib.vt.edu/iawa/ | **Required** |
    | CollectionCategory | The `VTDLP Access Website` site ID. e.g. `IAWA` | **Required** |
    | DYNOCollectionTABLE | collectiontablename | **Required** |
    | DYNOArchiveTABLE | archivetablename | **Required** |
    | DYNOCollectionmapTABLE | collectionmaptablename | **Required** |
    | NOIDNAA | The character string equivalent for the NAAN; for example, 13960 corresponds to the NAA, "archive.org" | **Required** |
    | NOIDScheme | ARK (Archival Resource Key) identifier scheme that the noid utility was partly created to support. E.g. `ark:/` | **Required** |
    | REGION | a valid AWS region. e.g. us-east-1 | **Required** |
    | LongURLPath | https://iawa.lib.vt.edu/ | **Required** |
    | ShortURLPath | http://idn.lib.vt.edu/ | **Required** |
    | APIKey | APIKEY | **Required** |
    | APIEndpoint | https://xxxx.execute-api.us-east-1.amazonaws.com/Prod/ | **Required** |
    | S3BucketName | An Amazon S3 bucket name for you to upload the metadata CSV file. This S3 bucket is not the same as `BUCKETNAME` and can not be an existing S3 bucket. | **Required** |
    | NoidLayerArn | A Lambda layer Arn. The value must be `arn:aws:lambda:us-east-1:909117335741:layer:noid-layer:6`. It is also the default value. | **Required** |

#### Step 3: Configure stack options
Leave it as is and click **Next**

#### Step 4: Review
Make sure all checkboxes under Capabilities section are **CHECKED**

Click *Create stack*

### Deploy VTDLP S3toDDB Lambda function using SAM CLI (For advanced users)

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
sam deploy --template-file packaged.yaml --stack-name STACKNAME --s3-bucket BUCKETNAME --parameter-overrides 'APPIMGROOTPATH=https://yourURL/ CollectionCategory=collection type DYNOCollectionTABLE=CollectionTableName DYNOArchiveTABLE=ArchiveTableName DYNOCollectionmapTABLE=CollectionmapTableName NOIDNAA=53696 NOIDScheme=ark:/ REGION=us-east-1 S3BucketName=S3BucketName LongURLPath=LongURLPath ShortURLPath=ShortURLPath APIKey=APIKey APIEndpoint=APIEndpoint' --capabilities CAPABILITY_IAM --region us-east-1
```

The above command will package and deploy your application to AWS, with a series of prompts:

- **Stack Name** (STACKNAME): (Required) The name of the AWS CloudFormation stack that you're deploying to. If you specify an existing stack, the command updates the stack. If you specify a new stack, the command creates it. This should be unique to your account and region, and a good starting point would be something matching your project name. Stack name can include letters (A-Z and a-z), numbers (0-9), and dashes (-).

- **S3 Bucket** (BUCKETNAME): (Required) An Amazon S3 bucket name where this command uploads your AWS CloudFormation template. S3 bucket name is globally unique, and the namespace is shared by all AWS accounts. See [Bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html). This S3 bucket should be already exist and you have the permission to upload files to it. This `BUCKETNAME` is a different S3 bucket, not the same S3 bucket as `S3BucketName`.

- **Parameter Overrides**: A string that contains AWS CloudFormation parameter overrides encoded as key-value pairs. For example, ParameterKey=ParameterValue NSTableName=DDBTableName.

    | Name | Description | Note |
    |----------|:-------------:|:-------------:|
    | APPIMGROOTPATH | Cloudfront URL which serves the static images. E.g. https://img.cloud.lib.vt.edu/iawa/ | **Required** |
    | CollectionCategory | The `VTDLP Access Website` site ID and it is case sensitive. e.g. `IAWA` | **Required** |
    | DYNOCollectionTABLE | collectiontablename | **Required** |
    | DYNOArchiveTABLE | archivetablename | **Required** |
    | DYNOCollectionmapTABLE | collectionmaptablename | **Required** |
    | NOIDNAA | The character string equivalent for the NAAN; for example, 13960 corresponds to the NAA, "archive.org" | **Required** |
    | NOIDScheme | ARK (Archival Resource Key) identifier scheme that the noid utility was partly created to support. E.g. `ark:/` | **Required** |
    | REGION | a valid AWS region. e.g. us-east-1 | **Required** |
    | LongURLPath | https://iawa.lib.vt.edu/ | **Required** |
    | ShortURLPath | http://idn.lib.vt.edu/ | **Required** |
    | APIKey | APIKEY | **Required** |
    | APIEndpoint | https://xxxx.execute-api.us-east-1.amazonaws.com/Prod/ | **Required** |
    | S3BucketName | An Amazon S3 bucket name for you to upload the metadata CSV file. This S3 bucket is not the same as `BUCKETNAME` and can not be an existing S3 bucket. | **Required** |
    | NoidLayerArn | A Lambda layer Arn. The value must be `arn:aws:lambda:us-east-1:909117335741:layer:noid-layer:6`.It is also the default value. | **Required** |

- **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modified IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command. [Learn more](https://docs.amazonaws.cn/en_us/serverlessrepo/latest/devguide/acknowledging-application-capabilities.html).
- **AWS Region**: The AWS region you want to deploy your app to.

### Usage
* Prepare "collection_metadata.csv" and "index.csv" for Collection and Item ingestion, respectively.
* Metadata ingestion:
    * For Collection ingestion: Set the filename as "collection_metadata.csv" and upload it to `S3BucketName`.
    * For Item ingestion: Set the filename as "index.csv." and upload it to `S3BucketName`.
* Go to DynamoDB to see the end results in Collection, Archive and Collectionmap tables.

## Tests

Tests are defined in the `tests` folder in this project. Use PIP to install the test dependencies and run tests. You 
must have a env file: [custom_pytest.ini.example](custom_pytest.ini.example)

```
python -m pytest --cov=. tests/unit -v -c custom_pytest.ini
```

These DynamoDB tables are used for testing:
```
archive_test
collection_test
collectionmap_test
```

These files are used for testing and stored in S3 bucket: vtdlp-dev-test
```
new_collection_metadata.csv
single_archive_metadata.csv
SFD_index.csv
```

Other test files are located in [tests/unit/test_data/](tests/unit/test_data/) folder

### Cleanup

To delete the sample application that you created, use the AWS CLI. Assuming you used your project name for the stack name, you can run the following:

```bash
aws cloudformation delete-stack --stack-name stackname
```
