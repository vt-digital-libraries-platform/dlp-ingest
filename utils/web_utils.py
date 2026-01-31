import logging, os, shutil, sys, yaml
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
    'COLLECTION_SUBDIRECTORY',
    'DRY_RUN',
    'DYNAMODB_TABLE_SUFFIX',
    'DYNAMODB_NOID_TABLE',
    'DYNAMODB_FILE_CHAR_TABLE',
    'ENV_SELECTION',
    'GENERATE_THUMBNAILS',
    'INGEST_TYPE',
    'ITEM_SUBDIRECTORY',
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
    '3D_OPTIONS-ROTATION-X',
    '3D_OPTIONS-ROTATION-Y',
    '3D_OPTIONS-SCALE',
    '3D_OPTIONS-ADDONS',
    '3D_OPTIONS-FLASH_CARD-OPTIONS-TEXT-FRONT',
    '3D_OPTIONS-FLASH_CARD-OPTIONS-TEXT-BACK',
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


def get_logfile(logger):
    for handler in logger.root.handlers:
        if isinstance(handler, logging.FileHandler):
            return handler.baseFilename
    return None


def set_environment(env_values):
    for key, value in env_values:
        if str(key).upper() in env_vars:
            ingestConfig[str(key).upper()] = value


def environment_json(env):
    envs = ["dev", "pprd", "prod"]
    env_json = {}
    for key in envs:
        env_json[key] = {}
        for field in env[key]:
            env_json[key] = env[field]
    return env_json


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
    set_environment_booleans()


def set_environment_booleans():
    logger.info(f"ingestConfig: {ingestConfig}")
    for item in request.form.items():
        logger.info(f"form: {item}")

    for key in env_vars:
        # if key not in ingestConfig.keys():
        #     logger.info(f"Not in ingestConfig")
        #     set_environment({key: False})
        # Convert string values to booleans
        # logger.info(f"{key}: {request.form.get(key)}")
        if key in ingestConfig and isinstance(ingestConfig[key], str) and ingestConfig[key].lower() == "true":
            ingestConfig[key] = True
        elif key in ingestConfig and isinstance(ingestConfig[key], str) and ingestConfig[key].lower() == "false":
            ingestConfig[key] = False


def get_identifier():
    return request.form.get('collection_identifier')


def save_uploads(application):
    files = []
    try:
        for file in request.files.getlist('metadata_input'):
            if file and file.filename.endswith(tuple(application.config['ALLOWED_EXTENSIONS'])):
                input_filename = get_input_filename(file)
                files.append(input_filename)
                file.save(os.path.join(application.config['UPLOADS'], f"{input_filename}"))
    except Exception as e:
        logger.error(f"Error: uploading file. - {e}")
    return files

def get_input_filename(file):
    return str(file.filename)


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