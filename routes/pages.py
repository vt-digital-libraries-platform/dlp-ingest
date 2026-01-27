import logging, os
from flask import redirect, render_template, request, session, url_for

from ingest import main as dlp_ingest_main
import utils.web_utils as utils

logger = logging.getLogger(__name__)

def index():
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
    ret_msgs = ["There was an exception processing your ingest. :( My bad. "]

    user = session.get('user')

    utils.set_environment_defaults(application)

    if request.method == 'POST' and 'metadata_input' in request.files:
        try:
            uploaded = utils.save_uploads(application)
        except Exception as e:
            err = "Error reading uploaded file"
            ret_msgs.append(err)
            logger.error(err)

        if utils.files_exist(application):
            utils.set_environment_overrides()

            # Do the ingest
            metadata_filepath = os.path.join(application.config['UPLOADS'], uploaded[0])

            result = None
            result = dlp_ingest_main(None, None, metadata_filepath, utils.get_ingestConfig())
            if result:
                ingested_items = result.get('ingested', [])
                updated_items = result.get('updated', [])
                errors = result.get('errors', [])
                summary = result.get('summary', [])
            else:
                ret_msgs.append("No response from ingest script dlp_ingest_main()")

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
                ret_msgs.append(f"Error writing results files: {e}")

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
                    log_lines = ["No log file found."]
                except Exception as e:
                    log_lines = [f"Error reading log file: {str(e)}"]

                return render_template(
                    'submit.html',
                    ingested_count=len(ingested_items),
                    updated_count=len(updated_items),
                    errors_count=len(errors),
                    summary_count=len(summary),
                    log_lines=log_lines,
                    user_is_admin=utils.user_is_admin(user)
                )
        else:
            ret_msgs.append("There was an exception finding the metadata file")
    else:
        ret_msgs.append(f"Incorrect request type: received GET. user: {user['email'] if user and "email" in user else "None"}")
   
    return redirect(url_for("index", msg=" || ".join(ret_msgs)))