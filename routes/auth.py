from flask import redirect, session, url_for
from authlib.integrations.flask_client import OAuth
import logging

logger = logging.getLogger(__name__)

def init(application, auth_config):
    oauth = OAuth(application)
    oauth.register(
        name='oidc',
        authority=auth_config['authority'],
        client_id=auth_config['client_id'],
        client_secret=auth_config['client_secret'],
        server_metadata_url=auth_config['metadata_url'],
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
        logger.error(f"authorize: {e}")
    return redirect(url_for('index'))


def logout():
    try:
        session.pop('user', None)
    except Exception as e:
        logger.error(f"logout: {e}")
    return redirect(url_for('index'))