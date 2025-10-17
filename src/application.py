import os, shutil, sys, yaml
from datetime import datetime
from flask import flash, Flask, render_template, request, jsonify, send_from_directory
import boto3
from src.dlp_ingest.lambda_function import main as dlp_ingest_main


application = Flask(__name__)
application.config['APPLICATION_ROOT'] = os.path.dirname(os.path.abspath(__file__))
application.config['STATIC'] = os.path.join(application.config['APPLICATION_ROOT'], 'static')
application.config['DEBUG'] = True
application.config['SECRET_KEY'] = os.urandom(24)
application.config['UPLOADS'] = os.path.join(application.config['APPLICATION_ROOT'], 'uploads')
application.config['ALLOWED_EXTENSIONS'] = {'csv'}

env_vars = [
    'VERBOSE',
    'AWS_SRC_BUCKET',
    'AWS_DEST_BUCKET',
    'COLLECTION_CATEGORY',
    'COLLECTION_IDENTIFIER',
    'COLLECTION_SUBDIRECTORY',
    'ITEM_SUBDIRECTORY',
    'REGION',
    'DYNAMODB_TABLE_SUFFIX',
    'DYNAMODB_NOID_TABLE',
    'DYNAMODB_FILE_CHAR_TABLE',
    'APP_IMG_ROOT_PATH',
    'NOID_SCHEME',
    'NOID_NAA',
    'LONG_URL_PATH',
    'SHORT_URL_PATH',
    'MEDIA_INGEST',
    'MEDIA_TYPE',
    'METADATA_INGEST',
    'GENERATE_THUMBNAILS',
    'DRY_RUN',
    'UPDATE_METADATA'
]

def cleanup(directory):
    try:
        shutil.rmtree(directory, ignore_errors=True)
        os.makedirs(directory)
    except:
        pass

cleanup(application.config['UPLOADS'])
    

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
            application.config[str(key).upper()] = value


def set_environment_defaults():
    defaults = None
    env_file = os.path.join(application.config['APPLICATION_ROOT'], 'static', 'yml', os.getenv('INGEST_ENV_YAML'))
    with open(env_file, 'r') as f:
        defaults = yaml.safe_load(f)
    if defaults:
        set_environment(defaults.items())
    else:
        print(f"Error loading environment defaults from {env_file}")
        sys.exit(1)


def set_environment_overrides():
    set_environment(request.form.items())


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
    return jsonify({'tables': response.get('TableNames', [])})


@application.route('/api/env_defaults')
def env_defaults():
    env_file = os.path.join(application.config['APPLICATION_ROOT'], 'static', 'yml', 'env_defaults.yml')
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

        application.config["VERBOSE"] = request.form.get("VERBOSE", "false") == "true"
        application.config["MEDIA_INGEST"] = request.form.get("MEDIA_INGEST", "false") == "true"
        application.config["METADATA_INGEST"] = request.form.get("METADATA_INGEST", "false") == "true"
        application.config["GENERATE_THUMBNAILS"] = request.form.get("GENERATE_THUMBNAILS", "false") == "true"
        application.config["DRY_RUN"] = request.form.get("DRY_RUN", "false") == "true"
        application.config["UPDATE_METADATA"] = request.form.get("UPDATE_METADATA", "false") == "true"
        application.config["IS_LAMBDA"] = request.form.get("IS_LAMBDA", "false") == "true"

        # Do the ingest
        metadata_filepath = os.path.join(application.config['UPLOADS'], uploaded[0])
        print(f"DEBUG: Calling dlp_ingest_main with file: {metadata_filepath}")
        result = dlp_ingest_main(None, None, metadata_filepath, application.config)
        print(f"DEBUG: Result returned by dlp_ingest_main: {result}")

        if result:
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
    return render_template('index.html')


@application.route('/success')
def success():
    return render_template('success.html')


@application.route('/results/<filename>')
def download_result(filename):
    results_dir = os.path.join(application.config['APPLICATION_ROOT'], 'results')
    print("Serving file:", os.path.join(results_dir, filename))
    return send_from_directory(results_dir, filename, as_attachment=True)



if __name__ == "__main__":
    set_environment_defaults()

    application.run(host='0.0.0.0', port=8000)