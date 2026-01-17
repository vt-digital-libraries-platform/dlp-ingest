import os
from flask import Flask

import routes.pages as pages
import routes.auth as auth


app_secret = os.environ.get('APP_SECRET')

application = Flask(__name__)
application.secret_key = app_secret

oauth = auth.init(application, app_secret)

# pages
application.add_url_rule('/', view_func=pages.index)
application.add_url_rule('/ingest-form', view_func=pages.ingestForm)


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