import boto3, logging, os, yaml
from flask import jsonify, request
import utils.web_utils as utils

logger = logging.getLogger(__name__)


def get_identifiers(application):
    identifiers = []
    suffix = request.args.get('suffix', '')
    if not suffix:
        return jsonify({'identifiers': []})
    table_name = f'Collection-{suffix}'

    dynamodb = boto3.resource('dynamodb', region_name=application.config.get('REGION', 'us-east-1'))
    table = dynamodb.Table(table_name)
    try:
        response = table.scan(ProjectionExpression='identifier')
        identifiers = [item['identifier'] for item in response.get('Items', [])]
    except Exception as e:
        logger.error(f"get_identifiers: {e}")

    return jsonify({'identifiers': identifiers})


def get_tables():
    tables = []
    try:
        dynamodb = boto3.client('dynamodb', region_name='us-east-1')
        response = dynamodb.list_tables()
        tables = utils.filterTableNames(response.get('TableNames', []))
    except Exception as e:
        logger.error(f"get_tables: {e}")

    return jsonify({'tables': tables})


def env_defaults(application):
    defaults = {}
    try:
        env_file = os.path.join(application.config['APP_SRC_DIR'], 'config', os.getenv('INGEST_ENV_YAML'))
        with open(env_file, 'r') as f:
            defaults = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"env_defaults: {e}")

    return jsonify(defaults)