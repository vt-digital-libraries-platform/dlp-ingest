import logging, os
from flask import Flask

import routes.pages as pages
import routes.auth as auth


logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='/var/log/application.log', 
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


# pages
application.add_url_rule('/', view_func=pages.index)
application.add_url_rule('/ingest_form', view_func=pages.ingest_form)

@application.route('/submit', methods=['GET', 'POST'])
def submit():
    pages.submit(application)


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



if __name__ == '__main__':
    application.run()