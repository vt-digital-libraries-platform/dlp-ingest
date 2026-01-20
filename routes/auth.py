from flask import redirect, session, url_for
from authlib.integrations.flask_client import OAuth
import logging

logger = logging.getLogger(__name__)

def init(application, app_secret):
    oauth = OAuth(application)
    oauth.register(
        name='oidc',
        authority='https://cognito-idp.us-east-1.amazonaws.com/us-east-1_wy1lPpMYt',
        client_id='4qicbtth4a9rhq6jrat24ic3oi',
        client_secret=app_secret,
        server_metadata_url='https://cognito-idp.us-east-1.amazonaws.com/us-east-1_wy1lPpMYt/.well-known/openid-configuration',
        client_kwargs={'scope': 'email openid'}
    )
    return oauth


def login(oauth):
    redirect_uri = url_for('authorize', _external=True)
    return oauth.oidc.authorize_redirect(redirect_uri)


def authorize(oauth):
    try:
        token = oauth.oidc.authorize_access_token()
        user = token['userinfo']
        session['user'] = user
    except Exception as e:
        logger.info(f"authorize: {e}")
    return redirect(url_for('index'))


def logout():
    try:
        session.pop('user', None)
    except Exception as e:
        logger.info(f"logout: {e}")
    return redirect(url_for('index'))