import os, shutil, sys, yaml
from datetime import datetime
from flask import flash, Flask, render_template, request, jsonify, send_from_directory
import boto3
from src.ingest import main as dlp_ingest_main

app_root = os.path.dirname(os.path.abspath(__file__))
application = Flask(__name__, template_folder=os.path.join(app_root, 'templates'))
application.config['APPLICATION_ROOT'] = app_root
application.config['STATIC'] = os.path.join(app_root, 'static')
application.config['DEBUG'] = True
application.config['SECRET_KEY'] = os.urandom(24)
application.config['UPLOADS'] = os.path.join(app_root, 'uploads')
application.config['ALLOWED_EXTENSIONS'] = {'csv'}
ingestConfig = {}


# empty directory (mostly for uploads)
def cleanup(directory):
    try:
        shutil.rmtree(directory, ignore_errors=True)
        os.makedirs(directory)
    except:
        pass
# and run it on startup
cleanup(application.config['UPLOADS'])


env_vars = [
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
    

def get_identifier():
    return request.form.get('collection_identifier')


def get_files():
    files = []
    try:
        files = [f for f in os.listdir(application.config['UPLOADS']) if os.path.isfile(os.path.join(application.config['UPLOADS'], f))]
    except Exception as e:
        pass
    return files


def files_exist():
    return len(get_files()) > 0



def get_input_filename(identifier, file, i, num_files):
    # indexPrefix = ""
    # for j in range(len(str(num_files)) - len(str(i))):
    #     indexPrefix += "0"
    # index = f"{indexPrefix}{i}"
    return str(file.filename)
    # return f"{identifier}_{index}_{file.filename}"


def save_uploads(identifier, num_files):
    files = []
    try:
        i = 0
        for file in request.files.getlist('metadata_input'):
            if file and file.filename.endswith(tuple(application.config['ALLOWED_EXTENSIONS'])):
                input_filename = get_input_filename(identifier, file, i, num_files)
                files.append(input_filename)
                file.save(os.path.join(application.config['UPLOADS'], f"{input_filename}"))
                i += 1
    except Exception as e:
        print(e)

    return files

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


def set_environment_defaults():
    defaults = None
    env_file = os.path.join(application.config['APPLICATION_ROOT'], 'config', os.getenv('INGEST_ENV_YAML'))
    with open(env_file, 'r') as f:
        defaults = yaml.safe_load(f)
    if defaults:
        set_environment(defaults.items())
    else:
        print(f"Error loading environment defaults from {env_file}")
        sys.exit(1)


def set_environment_overrides():
    set_environment(request.form.items())

    # ingestConfig["VERBOSE"] = request.form.get("VERBOSE", "false") == "true"
    # ingestConfig["MEDIA_INGEST"] = request.form.get("MEDIA_INGEST", "false") == "true"
    # ingestConfig["METADATA_INGEST"] = request.form.get("METADATA_INGEST", "false") == "true"
    # ingestConfig["GENERATE_THUMBNAILS"] = request.form.get("GENERATE_THUMBNAILS", "false") == "true"
    # ingestConfig["DRY_RUN"] = request.form.get("DRY_RUN", "false") == "true"
    # ingestConfig["UPDATE_METADATA"] = request.form.get("UPDATE_METADATA", "false") == "true"
    # ingestConfig["IS_LAMBDA"] = request.form.get("IS_LAMBDA", "false") == "true"


def get_available_envs():
    env_file = os.path.join(application.config['APPLICATION_ROOT'], "config", "available_envs.yml")
    with open(env_file, 'r') as f:
        envs = yaml.safe_load(f)

    return envs or []

def filterTableNames(table_names):
    envs = []
    for table in table_names:
        if table.startswith('Collection-'):
            if table not in envs:
                envs.append(table)

    return sorted(envs)


@application.route('/api/identifiers')
def get_identifiers():
    suffix = request.args.get('suffix', '')
    if not suffix:
        return jsonify({'identifiers': []})  # Or return an error message
    table_name = f'Collection-{suffix}'
    print(f"DEBUG: DynamoDB table name being used: {table_name}")

    dynamodb = boto3.resource('dynamodb', region_name=application.config.get('REGION', 'us-east-1'))
    table = dynamodb.Table(table_name)
    response = table.scan(ProjectionExpression='identifier')
    identifiers = [item['identifier'] for item in response.get('Items', [])]
    return jsonify({'identifiers': identifiers})


@application.route('/api/tables')
def get_tables():
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    response = dynamodb.list_tables()
    tables = filterTableNames(response.get('TableNames', []))
    return jsonify({'tables': tables})


@application.route('/api/env_defaults')
def env_defaults():
    env_file = os.path.join(application.config['APPLICATION_ROOT'], 'config', os.getenv('INGEST_ENV_YAML'))
    with open(env_file, 'r') as f:
        defaults = yaml.safe_load(f)

    return jsonify(defaults)


@application.route('/submit', methods=['GET', 'POST'])
def submit():
    uploaded = []
    timestamp = str(datetime.today()).replace(" ", "_")

    set_environment_defaults()
    collection_identifier = get_identifier()
    if request.method == 'POST' and 'metadata_input' in request.files:
        uploaded = save_uploads(collection_identifier, len(request.files.getlist('metadata_input')))

    ingested_items = []
    updated_items = []
    errors = []
    summary = []

    if files_exist():
        set_environment_overrides()

        # Do the ingest
        metadata_filepath = os.path.join(application.config['UPLOADS'], uploaded[0])
        print(f"DEBUG: Calling dlp_ingest_main with file: {metadata_filepath}")
        print(ingestConfig)

        result = None
        # result = dlp_ingest_main(None, None, metadata_filepath, application.config)
        if result:
            print(f"DEBUG: Result returned by dlp_ingest_main: {result}")
            ingested_items = result.get('ingested', [])
            updated_items = result.get('updated', [])
            errors = result.get('errors', [])
            summary = result.get('summary', [])
            print(f"DEBUG: ingested_items: {ingested_items}")
            print(f"DEBUG: updated_items: {updated_items}")
            print(f"DEBUG: errors: {errors}")
            print(f"DEBUG: summary: {summary}")

        # Write files for download
        results_dir = os.path.join(application.config['APPLICATION_ROOT'], 'results')
        os.makedirs(results_dir, exist_ok=True)

        with open(os.path.join(results_dir, 'ingested.csv'), 'w') as f:
            f.write("item\n")
            for item in ingested_items:
                f.write(f"{item}\n")

        with open(os.path.join(results_dir, 'updated.csv'), 'w') as f:
            f.write("item\n")
            for item in updated_items:
                f.write(f"{item}\n")

        with open(os.path.join(results_dir, 'errors.csv'), 'w') as f:
            f.write("error\n")
            for err in errors:
                f.write(f"{err}\n")

        with open(os.path.join(results_dir, 'summary.csv'), 'w') as f:
            f.write("summary\n")
            for line in summary:
                f.write(f"{line}\n")

        flash(f"Ingested:")
        flash(uploaded[0])
        flash(f"into collection: {collection_identifier}")
        
    return render_template(
        'submit.html',
        ingested_count=len(ingested_items),
        updated_count=len(updated_items),
        errors_count=len(errors),
        summary_count=len(summary)
    )


@application.route('/')
def index():
    envs = get_available_envs()
    set_environment_defaults()
    return render_template('index.html', envs=envs)


@application.route('/success')
def success():
    return render_template('success.html')


@application.route('/results/<filename>')
def download_result(filename):
    results_dir = os.path.join(application.config['APPLICATION_ROOT'], 'results')
    print("Serving file:", os.path.join(results_dir, filename))
    return send_from_directory(results_dir, filename, as_attachment=True)



if __name__ == "__main__":
    cleanup(application.config['UPLOADS'])
    set_environment_defaults()

    application.run(host='0.0.0.0', port=8000)