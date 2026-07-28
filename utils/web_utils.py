import logging, os, shutil, sys, yaml
import boto3
from flask import request

logger = logging.getLogger()

ingestConfig = {}

env_vars = [
    'APP_SRC_DIR',
    'APP_IMG_ROOT_PATH',
    'AWS_SRC_BUCKET',
    'AWS_DEST_BUCKET',
    'COLLECTION_CATEGORY',
    'COLLECTION_IDENTIFIER',
    'DRY_RUN',
    'DYNAMODB_TABLE_SUFFIX',
    'DYNAMODB_NOID_TABLE',
    'DYNAMODB_FILE_CHAR_TABLE',
    'EMBARGO_START_DATE',
    'EMBARGO_END_DATE',
    'ENV_SELECTION',
    'GENERATE_THUMBNAILS',
    'INGEST_TYPE',
    'LONG_URL_PATH',
    'MEDIA_INGEST',
    'MEDIA_TYPE',
    'METADATA_INGEST',
    'NOID_SCHEME',
    'NOID_NAA',
    'MEDIA_TYPE',
    'PARENT_COLLECTION_IDENTIFIER',
    'REGION',
    'SHORT_URL_PATH',
    'UPDATE_METADATA',
    'VERBOSE',
    'VISIBILITY',
    '3D_OPTIONS_ROTATION_X',
    '3D_OPTIONS_ROTATION_Y',
    '3D_OPTIONS_SCALE',
    '3D_OPTIONS_ADDONS',
    '3D_OPTIONS_FLASH_CARD_OPTIONS_TEXT_FRONT',
    '3D_OPTIONS_FLASH_CARD_OPTIONS_TEXT_BACK',
]


def check_messages():
    msg = None
    try:
        msg = request.args.get('msg', None)
    except Exception as e:
        logger.info(f"index: {e}")
    return msg


def get_ingestConfig():
    return ingestConfig


def get_available_envs(application):
    env_file = os.path.join(application.config['APP_SRC_DIR'], "config", "available_envs.yml")
    with open(env_file, 'r') as f:
        envs = yaml.safe_load(f)

    return envs or []


def clear_logfile(logger):
    logfile = get_logfile(logger)
    if logfile:
        with open(logfile, 'w'):
            pass


def get_logfile(logger):
    for handler in logger.root.handlers:
        if isinstance(handler, logging.FileHandler):
            return handler.baseFilename
    return None


def environment_json(env):
    envs = ["dev", "pprd", "prod"]
    env_json = {}
    for key in envs:
        env_json[key] = {}
        for field in env[key]:
            env_json[key] = env[field]
    return env_json


def set_environment(env_values):
    for key, value in env_values:
        if str(key).upper() in env_vars:
            # convert string booleans from form into actual booleans
            if isinstance(value, str) and value.lower() == "true":
                ingestConfig[str(key).upper()] = True
            elif isinstance(value, str) and value.lower() == "false":
                ingestConfig[str(key).upper()] = False
            # or just add the value to the config as is
            else:
                ingestConfig[str(key).upper()] = value


def set_environment_defaults(application):
    defaults = None
    env_file = os.path.join(application.config['APP_SRC_DIR'], 'config', os.getenv('INGEST_ENV_YAML'))
    
    try:
        with open(env_file, 'r') as f:
            defaults = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"set_environment_defaults: {e}")

    if defaults:
        set_environment(defaults.items())
        set_environment({'APP_SRC_DIR': application.config['APP_SRC_DIR']}.items())
    else:
        logger.info(f"Error loading environment defaults from {env_file}")


def set_environment_overrides():
    set_environment(request.form.items())


def get_identifier():
    return request.form.get('collection_identifier')


def save_uploads(application, field_name='metadata_input', allowed_extensions=None):
    files = []
    try:
        for file in request.files.getlist(field_name):
            if not file or not file.filename:
                continue

            filename = get_input_filename(file)
            if allowed_extensions:
                normalized_name = filename.lower()
                if not normalized_name.endswith(tuple(f".{ext.lower().lstrip('.')}" for ext in allowed_extensions)):
                    continue

            files.append(filename)
            file.save(os.path.join(application.config['UPLOADS'], filename))
    except Exception as e:
        logger.error(f"Error: uploading file. - {e}")
    return files

def get_input_filename(file):
    return os.path.basename(str(file.filename))


def upload_files_to_collection_root(filepaths, ingest_config):
    uploaded_locations = []

    collection_category = ingest_config.get("COLLECTION_CATEGORY")
    collection_identifier = ingest_config.get("COLLECTION_IDENTIFIER")
    src_bucket = ingest_config.get("AWS_SRC_BUCKET")
    dest_bucket = ingest_config.get("AWS_DEST_BUCKET")
    region = ingest_config.get("REGION")

    if not collection_category or not collection_identifier:
        logger.error("Missing COLLECTION_CATEGORY or COLLECTION_IDENTIFIER in ingest config; skipping checksum manifest upload")
        return uploaded_locations

    if not src_bucket or not dest_bucket:
        logger.error("Missing AWS_SRC_BUCKET or AWS_DEST_BUCKET in ingest config; skipping checksum manifest upload")
        return uploaded_locations

    collection_root = os.path.join(collection_category, collection_identifier)
    s3_client = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    dry_run = bool(ingest_config.get("DRY_RUN"))

    for filepath in filepaths:
        filename = os.path.basename(filepath)
        if not filename:
            continue
        key = os.path.join(collection_root, filename).replace("\\", "/")

        for bucket in [src_bucket, dest_bucket]:
            if dry_run:
                logger.info(f"DRYRUN: checksum manifest upload simulated for s3://{bucket}/{key}")
                uploaded_locations.append(f"s3://{bucket}/{key}")
                continue

            try:
                s3_client.upload_file(filepath, bucket, key)
                uploaded_locations.append(f"s3://{bucket}/{key}")
            except Exception as e:
                logger.error(f"Error uploading checksum manifest {filename} to {bucket}/{key}: {e}")

    return uploaded_locations


def files_exist(application):
    return len(get_files(application)) > 0


def get_files(application):
    files = []
    try:
        files = [f for f in os.listdir(application.config['UPLOADS']) if os.path.isfile(os.path.join(application.config['UPLOADS'], f))]
    except Exception as e:
        pass
    return files


def user_is_admin(user):
    return (
        user and 
        'cognito:groups' in user and 
        "admin" in user['cognito:groups']
    )


# empty directory (mostly for uploads)
def cleanup(directory):
    try:
        shutil.rmtree(directory, ignore_errors=True)
        os.makedirs(directory)
    except:
        pass


def filterTableNames(table_names):
    envs = []
    for table in table_names:
        if table.startswith('Collection-'):
            if table not in envs:
                envs.append(table)

    return sorted(envs)