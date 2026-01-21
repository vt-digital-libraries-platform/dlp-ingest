# DLP Ingest UI

A web-based user interface for ingesting digital library metadata into DynamoDB, built with Flask and designed with WCAG 2.1 Level AA accessibility compliance.

## Overview

The DLP Ingest UI provides a web form for uploading CSV metadata files to DynamoDB. You select an environment (dev, preprod, or prod), choose whether you're uploading collection or archive metadata, upload your CSV file, and the system processes it.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.12** or higher
- **pip** (Python package manager)
- **AWS CLI** configured with appropriate credentials
- **Git** for version control

### AWS Permissions Required

Your AWS credentials must have permissions for:

- DynamoDB (read/write access to Collection and Archive tables)
- S3 (read/write access to source and destination buckets)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/vt-digital-libraries-platform/dlp-ingest.git
cd dlp-ingest
git checkout ui-merge-lee
```

### 2. Create and Activate Virtual Environment

```bash
python3 -m venv .
source bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

Create an environment configuration file at `src/config/env_defaults.yml`:

```yaml
dev:
  aws_src_bucket: "<your-dev-source-bucket>"
  aws_dest_bucket: "<your-dev-destination-bucket>"
  collection_category: "<your-collection-category>"
  dynamodb_noid_table: "<your-dev-noid-table>"
  dynamodb_file_char_table: "<your-dev-file-char-table>"
  app_img_root_path: "<your-cloudfront-endpoint>"
  long_url_path: "<your-dev-site-url>"
  short_url_path: "<your-dev-short-url>"
  noid_scheme: "ark:/"
  noid_naa: "<your-naa-number>"

preprod:
  aws_src_bucket: "<your-preprod-source-bucket>"
  aws_dest_bucket: "<your-preprod-destination-bucket>"
  collection_category: "<your-collection-category>"
  dynamodb_noid_table: "<your-preprod-noid-table>"
  dynamodb_file_char_table: "<your-preprod-file-char-table>"
  app_img_root_path: "<your-cloudfront-endpoint>"
  long_url_path: "<your-preprod-site-url>"
  short_url_path: "<your-preprod-short-url>"
  noid_scheme: "ark:/"
  noid_naa: "<your-naa-number>"

prod:
  aws_src_bucket: "<your-prod-source-bucket>"
  aws_dest_bucket: "<your-prod-destination-bucket>"
  collection_category: "<your-collection-category>"
  dynamodb_noid_table: "<your-prod-noid-table>"
  dynamodb_file_char_table: "<your-prod-file-char-table>"
  app_img_root_path: "<your-cloudfront-endpoint>"
  long_url_path: "<your-prod-site-url>"
  short_url_path: "<your-prod-short-url>"
  noid_scheme: "ark:/"
  noid_naa: "<your-naa-number>"
```

Replace the placeholder values (anything in `<angle brackets>`) with your actual AWS bucket names, DynamoDB table names, and URLs.

### 5. Configure AWS Credentials (One-Time Setup)

If you haven't already, configure your AWS credentials. This is a one-time setup that stores your credentials in `~/.aws/credentials`:

```bash
aws configure
```

You'll be prompted to enter:

- AWS Access Key ID
- AWS Secret Access Key
- Default region (use `us-east-1`)
- Default output format (press Enter to skip)

**The application will automatically use these credentials**

## Running the Application

### Quick Start

Use the provided startup script:

```bash
bash dev_startup.sh
```

This will:

1. Activate the virtual environment
2. Install/update dependencies
3. Start the Gunicorn server on `http://localhost:8002`

### Manual Start

Alternatively, start the server manually:

```bash
source bin/activate
gunicorn -w 1 --threads 15 -b 0.0.0.0:8002 src.application:application
```

### Accessing the Application

Open your web browser and navigate to:

```
http://localhost:8002
```

## Using the Application

### 1. Select DynamoDB Table

The first dropdown allows you to select your target DynamoDB table. Choose from available Collection or Archive tables.

**Tip:** The table name format is `Collection-{suffix}` or `Archive-{suffix}`, where the suffix indicates the environment (e.g., `vtdlpdev`, `vtdlppprd`, `vtdlpprd`).

### 2. Select Ingest Type

Choose the type of ingest operation:

- **Items/Archives**: Ingest individual item or archive metadata
- **Collection(s)**: Ingest collection-level metadata

### 3. Select Environment

Choose the environment you're ingesting to (Dev, Preprod, or Prod) using the radio buttons. This will automatically populate all form fields with environment-specific defaults from your `env_defaults.yml` configuration.

You can either:

- **Select a predefined environment** (Dev, Preprod, Prod) - recommended
- **Select "Other"** - manually configure all settings

**Note:** The environment selection is separate from the table selection in step 1. The table dropdown determines which specific DynamoDB table to use, while the environment selection populates all the other form fields (S3 buckets, URLs, etc.) with your configured defaults.

### 4. Configure Settings

Fill in the required fields across the form sections:

- **Collection Configuration**: Category and identifiers
- **AWS Configuration**: Source and destination S3 buckets
- **DynamoDB Configuration**: NOID and file character tables
- **Endpoints**: CloudFront endpoint, site URL, and short URL path
- **NOID Configuration**: Scheme and NAA (Name Assigning Authority)
- **Media Configuration**: Media type (image, video, audio, etc.)

**Auto-Population:** Many fields are automatically populated based on the selected environment. Review and adjust as needed.

### 5. Upload Metadata File

Click "Choose File" and select your CSV metadata file. The file must include:

- Required column: `identifier`
- Additional columns specific to your media type and collection requirements

### 6. Submit

Click "Start Ingest" to begin the upload. A progress bar will show the upload status.

## Viewing Ingest Results

After the ingest completes, you'll be redirected to a success page that shows:

- **Ingest Status**: Confirmation that the upload completed
- **Log Output**: The last 100 lines from `startup.log` showing any errors, warnings, or processing details from the ingest operation

The log output is displayed in a scrollable box, allowing you to review what happened during the ingest process without needing to check the log file manually.

## Form Sections Status

Each section shows a status indicator:

- ✅ **Complete** (green): All required fields are filled
- ⚠️ **Incomplete (X/Y)** (orange): Shows how many required fields are filled

Click "Go to Next Incomplete Section" to navigate to missing fields.

## Accessibility Features

This application is designed with WCAG 2.1 Level AA compliance:

- **High Contrast**: Minimum 4.5:1 contrast ratio for all text
- **Semantic HTML**: Proper use of headings, fieldsets, and legends
- **Keyboard Navigation**: All functions accessible via keyboard
- **Large Text**: Base font size of 1.25em for better readability
- **Form Labels**: All inputs properly labeled with descriptive text

## Troubleshooting

### Port Already in Use

If port 8002 is already in use:

```bash
# Kill processes on port 8002
pkill -f "gunicorn.*8002"
lsof -ti:8002 | xargs kill -9
```

### AWS Credentials Error

Ensure your AWS credentials are properly configured:

```bash
aws configure
# Enter your Access Key ID, Secret Access Key, and region
```

### DynamoDB Table Not Found

Verify that:

1. Your AWS credentials have DynamoDB access
2. You're in the correct AWS region (us-east-1)
3. The table names match your environment configuration

### Virtual Environment Issues

If you encounter package import errors:

```bash
deactivate
rm -rf lib/ bin/ include/
python3 -m venv .
source bin/activate
pip install -r requirements.txt
```

### Server Won't Start

Check the `startup.log` file for detailed error messages:

```bash
tail -100 startup.log
```

## Development

### File Structure

```
dlp-ingest/
├── src/
│   ├── application.py           # Flask app and routes
│   ├── ingest.py               # Core ingest logic
│   ├── config/
│   │   ├── available_envs.yml  # Environment configurations
│   │   └── env_defaults.yml    # Default values per environment
│   ├── ingest_classes/
│   │   └── metadata/
│   │       └── generic_metadata.py  # Metadata handling
│   ├── static/
│   │   ├── css/
│   │   │   └── style.css       # WCAG-compliant styles
│   │   └── js/
│   │       └── index.js        # Client-side functionality
│   └── templates/
│       └── index.html          # Main form interface
├── dev_startup.sh              # Development startup script
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

### Making Changes

1. Create a feature branch from `ui-merge-lee`
2. Make your changes
3. Test locally using `dev_startup.sh`
4. Commit with descriptive messages
5. Push and create a pull request

## Environment Configuration Reference

Each environment requires these settings:

| Setting                    | Description                     |
| -------------------------- | ------------------------------- |
| `aws_src_bucket`           | Source S3 bucket for uploads    |
| `aws_dest_bucket`          | Destination S3 bucket           |
| `collection_category`      | Collection category             |
| `dynamodb_noid_table`      | NOID minting table              |
| `dynamodb_file_char_table` | File characteristics table      |
| `app_img_root_path`        | CloudFront endpoint             |
| `long_url_path`            | Full site URL                   |
| `short_url_path`           | Short URL path                  |
| `noid_scheme`              | NOID scheme                     |
| `noid_naa`                 | Name Assigning Authority number |

## Support

For issues or questions:

- Check existing issues: https://github.com/vt-digital-libraries-platform/dlp-ingest/issues
- Create a new issue with detailed information about your problem
- Include error messages, logs, and steps to reproduce

## License

- **MIT License**

## Contributors

Virginia Tech Digital Libraries Platform Team
