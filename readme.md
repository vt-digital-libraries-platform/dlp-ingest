# S3toDDB
CSV file upload to a S3 bucket and store the data into DynamoDB

# Lambda function
* [lambda_function.py](lambda_function.py)

# Installation
* Runtime: Python 3.7
* Layers: arn:aws:lambda:us-east-1:xxxxxxxxx:layer:iawa-layer:1 See [IAWA layer](https://github.com/vt-digital-libraries-platform/lambda_layers)

# Environment variables
| Key | Value |
|----------|:-------------:|
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

# CodeBuild 
* buildspec.yml: Update [BUCKET](buildspec.yml#L11)

# CodeDeploy Parameter Override
* example

```
{"APPIMGROOTPATH":"https://img.cloud.lib.vt.edu/iawa/","BibliographicCitation":"Researchers wishing to cite this collection should include the following information: - Special Collections, Virginia Polytechnic Institute and State University, Blacksburg, Va.","CollectionCategory":"IAWA","DYNOCollectionTABLE":"Collection-xxxxxx","DYNOArchiveTABLE":"Archive-yyyyyy","NOIDNAA":"53696","NOIDScheme":"ark:/","NOID_Template":"eeddeede","REGION":"us-east-1","RightsHolder":"Special Collections, University Libraries, Virginia Tech","RightsStatement":"Permission to publish material from the must be obtained from University Libraries Special Collections, Virginia Tech.","S3BucketName":"iawa-s3csv","LambdaLayerParameter":"arn:aws:lambda:us-east-1:xxxxxxxxx:layer:iawa-layer:1"}
```

# Redeploy
* Empty and delete the S3 bucket storing the CSV files
* Delete cloudformation stack
```
aws cloudformation delete-stack --stack-name iawa-metadata
aws cloudformation describe-stacks --stack-name iawa-metadata
```

# Test
* Create a S3 bucket to store "collection_metadata.csv" and "index.csv" for Collection and Item ingestion, respectively.
* Create a test event in Lambda function. Set the S3 bucket name as in the previous step. For Collection ingestion, set the object key as "collection_metadata.csv"; for Item ingestion, set the object key as "index.csv."
* Go to DynamoDB to see the end results in Collection and Archive tables
