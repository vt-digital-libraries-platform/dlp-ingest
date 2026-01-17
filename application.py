from flask import Flask, redirect, render_template, url_for, session
from authlib.integrations.flask_client import OAuth
import os


app_secret = os.environ.get('APP_SECRET')

application = Flask(__name__)
application.secret_key = app_secret

oauth = OAuth(application)
oauth.register(
  name='oidc',
  authority='https://cognito-idp.us-east-1.amazonaws.com/us-east-1_wy1lPpMYt',
  client_id='4qicbtth4a9rhq6jrat24ic3oi',
  client_secret=app_secret,
  server_metadata_url='https://cognito-idp.us-east-1.amazonaws.com/us-east-1_wy1lPpMYt/.well-known/openid-configuration',
  client_kwargs={'scope': 'email openid'}
)


@application.route('/')
def index():
    user = session.get('user')
    if user:
        return  f'Hello, {user["email"]}. <a href="/logout">Logout</a>'
    else:
        return render_template("login_page.html")
    

@application.route('/ingest-form')
def ingestForm():
    user = session.get('user')
    return render_template("form.html", user=user)
    
# //////////////////// Auth Routes ////////////////////////////////
    
@application.route('/login')
def login():
    redirect_uri = url_for('authorize', _external=True)
    return oauth.oidc.authorize_redirect(redirect_uri)


@application.route('/authorize')
def authorize():
    try:
        token = oauth.oidc.authorize_access_token()
        user = token['userinfo']
        session['user'] = user
    except Exception as e:
        print(e)
    return redirect(url_for('index'))


@application.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('index'))


# ////////////////////////////////////////////////////



if __name__ == '__main__':
    application.run()