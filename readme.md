# S3toDDB
CSV file upload to a S3 bucket and store the data into DynamoDB

# Lambda function
* [lambda_function.py](lambda_function.py)

# Installation
* Runtime: Python 3.7
* Layers: arn:aws:lambda:us-east-1:xxxxxxxxx:layer:iawa-layer:1 See [Create a Lambda layer](https://github.com/VTUL/S3toDDB#create-a-lambda-layer)

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
{"APP_IMG_ROOT_PATH":"https://img.cloud.lib.vt.edu/iawa/","Bibliographic_Citation":"Researchers wishing to cite this collection should include the following information: - Special Collections, Virginia Polytechnic Institute and State University, Blacksburg, Va.","Collection_Category":"IAWA","DYNO_Collection_TABLE":"Collection-xxxxxx","DYNO_Archive_TABLE":"Archive-yyyyyy","NOID_NAA":"53696","NOID_Scheme":"ark:/","NOID_Template":"eeddeede","REGION":"us-east-1","Rights_Holder":"Special Collections, University Libraries, Virginia Tech","Rights_Statement":"Permission to publish material from the must be obtained from University Libraries Special Collections, Virginia Tech.","S3BucketName":"iawa-s3csv","LambdaLayerParameter":"arn:aws:lambda:us-east-1:xxxxxxxxx:layer:iawa-layer:1"}
```

# Create a Lambda layer
* Create a Lambda layer using CLI
```
aws lambda publish-layer-version --layer-name iawa-layer --description "IAWA layer" --zip-file fileb://iawa-layer.zip --compatible-runtimes python3.7
```
Output
```
{
    "Content": {
        "Location": "....",
        "CodeSha256": "xxxxxxxx+LyoETfaKSxxxxxxx",
        "CodeSize": 31585176
    },
    "LayerArn": "arn:aws:lambda:us-east-1:xxxx:layer:iawa-layer",
    "LayerVersionArn": "arn:aws:lambda:us-east-1:xxxx:layer:iawa-layer:1",
    "Description": "IAWA layer",
    "CreatedDate": "2020-02-12T03:32:10.694+0000",
    "Version": 1,
    "CompatibleRuntimes": [
        "python3.7"
    ]
}
```

# Redeploy
* Empty files in the source S3 bucket
* Delete cloudformation stack
```
aws cloudformation delete-stack --stack-name iawa-metadata
aws cloudformation describe-stacks --stack-name iawa-metadata
```

# Test
* Create a S3 bucket to store "collection_metadata.csv" and "index.csv" for Collection and Item ingestion, respectively.
* Create a test event in Lambda function. Set the S3 bucket name as in the previous step. For Collection ingestion, set the object key as "collection_metadata.csv"; for Item ingestion, set the object key as "index.csv."
* Go to DynamoDB to see the end results in Collection and Archive tables
