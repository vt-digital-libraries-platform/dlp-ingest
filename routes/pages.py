import logging, os
from flask import redirect, render_template, request, session, url_for

from ingest import main as dlp_ingest_main
import utils.web_utils as utils

logger = logging.getLogger(__name__)

def index():
    if os.environ.get('LOCAL_DEV') == 'true':
        session['user'] = {"email": "user@email.com","cognito:groups": ["admin"]}
    user = session.get('user')
    msg = utils.check_messages()
    if user:
        if utils.user_is_admin(user):
            return  redirect(url_for("ingest_form", msg=msg))
        else:
            if not msg:
                msg = f"Hi {user['email']}! Please contact a DLP team member for admin privileges"
            return render_template("index.html", msg=msg, user=user)
    else:
        return render_template("index.html", msg=msg)
    

def ingest_form(application):
    user = session.get('user')
    msg = utils.check_messages()
    if(utils.user_is_admin(user) or os.getenv('LOCAL_DEV') == "true"):
        envs = utils.get_available_envs(application)
        return render_template("form.html", envs=envs, user=user, msg=msg) 
    else:
        return redirect(url_for("index", msg="Not authorized to access page. Please login."))


def submit(application):

    uploaded = []
    ingested_items = []
    updated_items = []
    errors = []
    summary = []

    logger.info("====================================================")
    logger.info("/submit -- received ingest request. Beginning ingest process")
    logger.info("====================================================")
    logger.info(request)

    user = session.get('user')
    if user:
        logger.info(f"User: {user['email']}")
    else:
        logger.error("No user session")

    utils.set_environment_defaults(application)

    if request.method == 'POST' and 'metadata_input' in request.files:
        try:
            uploaded = utils.save_uploads(application)
            logger.info(f"Metadata file uploaded: {uploaded}")
        except Exception as e:
            err = "Error reading uploaded file"
            logger.error(err)

        if utils.files_exist(application):
            utils.set_environment_overrides()

            # Do the ingest
            metadata_filepath = os.path.join(application.config['UPLOADS'], uploaded[0])
            ingestConfig = utils.get_ingestConfig()
            logger.info(f"Config: {ingestConfig}")
            logger.info("BEGIN INGEST RESULTS---------------------")
            result = dlp_ingest_main(None, None, metadata_filepath, ingestConfig)
            logger.info("--------------------- ...END INGEST RESULTS")
            if result:
                ingested_items = result.get('ingested', [])
                updated_items = result.get('updated', [])
                errors = result.get('errors', [])
                summary = result.get('summary', [])
            else:
                err = "No return value from ingest script dlp_ingest_main()"
                logger.error(err)

            # Write files for download
            results_dir = os.path.join(application.config['APP_SRC_DIR'], 'results')
            os.makedirs(results_dir, exist_ok=True)
            try:
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

            except Exception as e:
                err = f"Error writing results files: {e}"
                logger.error(err)

            # Read the last 100 lines of log_file to show ingest logs
            # Get log_file path from logger config
            log_file = utils.get_logfile(logger)
            if log_file:
                log_lines = []
                try:
                    with open(log_file, 'r') as f:
                        all_lines = f.readlines()
                        # Get last 100 lines, or all if fewer than 100
                        log_lines = all_lines[-100:] if len(all_lines) > 100 else all_lines
                except FileNotFoundError:
                    err = "No log file found."
                    logger.error(err)
                    log_lines = [err]
                except Exception as e:
                    err = f"Error reading log file: {str(e)}"
                    logger.error(err)
                    log_lines = [err]
                return render_template(
                    'submit.html',
                    user=user,
                    user_is_admin=utils.user_is_admin(user),
                    ingested_count=len(ingested_items),
                    updated_count=len(updated_items),
                    errors_count=len(errors),
                    summary_count=len(summary),
                    log_lines=log_lines
                )
        else:
            err = "There was an exception finding the metadata file"
            logger.error(err)
    else:
        logger.info("/submit received GET. Redirecting home")
   
    return redirect(url_for("index", msg="There was an exception in the process. Please check the logs. ...my bad"))