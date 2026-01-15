from flask import Flask, redirect, url_for, session
from authlib.integrations.flask_client import OAuth
import os

application = Flask(__name__)
application.secret_key = os.environ.get('FLASK_SECRET') or "94e7f486-36f6-4fdb-b2c3-83a486ac69b1"

oauth = OAuth(application)
oauth.register(
  name='oidc',
  authority='https://cognito-idp.us-east-1.amazonaws.com/us-east-1_wy1lPpMYt',
  client_id='4qicbtth4a9rhq6jrat24ic3oi',
  client_secret='163q19s9uqsqdus627mbhg4m6ajnpeepheonacflg17q180volbn',
  server_metadata_url='https://cognito-idp.us-east-1.amazonaws.com/us-east-1_wy1lPpMYt/.well-known/openid-configuration',
  client_kwargs={'scope': 'email openid'}
)


@application.route('/')
def index():
    user = session.get('user')
    if user:
        return  f'Hello, {user["email"]}. <a href="/logout">Logout</a>'
    else:
        redirect_uri = url_for('authorize', _external=True)
        markup = '<p>Welcome! Please <a href="/login">Login</a>.</p>'
        markup += f"Auth route: {redirect_uri}"
        return markup
    

@application.route('/login')
def login():
    # Alternate option to redirect to /authorize
    redirect_uri = url_for('authorize', _external=True)
    return oauth.oidc.authorize_redirect(redirect_uri)
    # return oauth.oidc.authorize_redirect('http://localhost:8000/')


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

if __name__ == '__main__':
    application.run()