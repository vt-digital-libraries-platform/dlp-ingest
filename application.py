import logging, os
from flask import Flask

import routes.pages as pages
import routes.auth as auth
import routes.api as api

import utils.web_utils as utils


logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.environ.get('LOG_FILE') or '/var/log/application.log', 
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger.info("----------------------------------------")
logger.info("Starting instance...")

app_secret = os.environ.get('APP_SECRET')

application = Flask(__name__)
application.secret_key = app_secret

oauth = auth.init(application, app_secret)

app_src_dir = os.path.dirname(os.path.abspath(__file__))
application.config['APP_SRC_DIR'] = app_src_dir

application.config['STATIC'] = os.path.join(app_src_dir, 'static')
application.config['UPLOADS'] = os.path.join(app_src_dir, 'uploads')
application.config['TEMPLATE_DIR'] = os.path.join(app_src_dir, 'templates')
application.config['ALLOWED_EXTENSIONS'] = {'csv'}
application.config['DEBUG'] = True

# empty upload directory on startup
utils.cleanup(application.config['UPLOADS'])


# === Routes === 

# pages
application.add_url_rule('/', view_func=pages.index)

@application.route('/ingest_form')
def ingest_form():
    return pages.ingest_form(application)

@application.route('/submit', methods=['GET', 'POST'])
def submit():
    return pages.submit(application)


#auth
@application.route('/login')
def login():
    return auth.login(oauth)

@application.route('/authorize')
def authorize():
    return auth.authorize(oauth)

@application.route('/logout')
def logout():
    return auth.logout()


# api
@application.route('/api/identifiers')
def get_identifiers():
    return api.get_identifiers(application)

@application.route('/api/tables')
def get_tables():
    return api.get_tables()

@application.route('/api/env_defaults')
def env_defaults():
    return api.env_defaults(application)


if __name__ == '__main__':
    application.run()