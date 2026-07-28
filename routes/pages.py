import logging, os
from flask import redirect, render_template, request, session, url_for

from ingest import main as dlp_ingest_main
import utils.web_utils as utils

logger = logging.getLogger(__name__)

def index():
    # Create a user for local dev, if appropriate.
    # If someone has enough permissions to set env vars on our machines, 
    # they probably have more interesting things to do than mess with this.
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
    if utils.user_is_admin(user):
        envs = utils.get_available_envs(application)
        return render_template("form.html", envs=envs, user=user, msg=msg) 
    else:
        return redirect(url_for("index", msg="Not authorized to access page. Please login."))


def submit(application):

    collection_uploaded = []
    archive_uploaded = []
    checksum_uploaded = []
    ingested_items = []
    updated_items = []
    errors = []
    summary = []

    # Clear the logfile each run
    utils.clear_logfile(logger)

    logger.info("====================================================")
    logger.info("/submit -- received ingest request. Beginning ingest process")
    logger.info("====================================================")

    user = session.get('user')
    if user:
        logger.info(f"User: {user['email']}")
    else:
        logger.error("No user session")

    utils.set_environment_defaults(application)

    if request.method == 'POST':
        ingest_type = (request.form.get('INGEST_TYPE') or 'archive').lower()
        try:
            collection_uploaded = utils.save_uploads(
                application,
                field_name='collection_metadata_input',
                allowed_extensions=application.config['ALLOWED_EXTENSIONS']
            )
            if collection_uploaded:
                logger.info(f"Collection metadata file uploaded: {collection_uploaded}")

            archive_uploaded = utils.save_uploads(
                application,
                field_name='archive_metadata_input',
                allowed_extensions=application.config['ALLOWED_EXTENSIONS']
            )
            if archive_uploaded:
                logger.info(f"Archive metadata file uploaded: {archive_uploaded}")

            checksum_uploaded = utils.save_uploads(
                application,
                field_name='checksum_manifest_input',
                allowed_extensions=application.config['ALLOWED_EXTENSIONS']
            )
            if checksum_uploaded:
                logger.info(f"Checksum manifest file(s) uploaded locally: {checksum_uploaded}")
        except Exception as e:
            err = "Error reading uploaded file"
            logger.error(err)

        selected_metadata_filename = None
        if ingest_type == 'collection':
            if collection_uploaded:
                selected_metadata_filename = collection_uploaded[0]
            else:
                err = "Collection ingest requires a collection metadata CSV file"
                logger.error(err)
        else:
            if archive_uploaded:
                selected_metadata_filename = archive_uploaded[0]
            else:
                err = "Archive ingest requires an item/archive metadata CSV file"
                logger.error(err)

        utils.set_environment_overrides()
        ingestConfig = utils.get_ingestConfig()

        metadata_filepaths = [
            os.path.join(application.config['UPLOADS'], filename)
            for filename in (collection_uploaded + archive_uploaded)
        ]
        metadata_filepaths = list(dict.fromkeys(metadata_filepaths))
        if metadata_filepaths:
            metadata_locations = utils.upload_files_to_collection_root(
                metadata_filepaths,
                ingestConfig,
                prepend_date_if_missing=True,
                log_label='metadata file'
            )
            logger.info(f"Metadata files uploaded to collection root: {metadata_locations}")

        if selected_metadata_filename:

            collection_metadata_filepath = (
                os.path.join(application.config['UPLOADS'], collection_uploaded[0])
                if collection_uploaded
                else None
            )
            archive_metadata_filepath = (
                os.path.join(application.config['UPLOADS'], archive_uploaded[0])
                if archive_uploaded
                else None
            )
            ingestConfig['COLLECTION_METADATA_FILEPATH'] = collection_metadata_filepath
            ingestConfig['ARCHIVE_METADATA_FILEPATH'] = archive_metadata_filepath

            if checksum_uploaded:
                checksum_filepaths = [
                    os.path.join(application.config['UPLOADS'], filename)
                    for filename in checksum_uploaded
                ]
                checksum_locations = utils.upload_files_to_collection_root(
                    checksum_filepaths,
                    ingestConfig,
                    log_label='checksum manifest'
                )
                logger.info(f"Checksum manifest uploaded to collection root: {checksum_locations}")

            # Do the ingest
            metadata_filepath = os.path.join(application.config['UPLOADS'], selected_metadata_filename)
            logger.info(f"Config: {ingestConfig}")
            logger.info("INGEST RESULTS---------------------")
            result = dlp_ingest_main(None, None, metadata_filepath, ingestConfig)
            logger.info("--------------------- ...END INGEST RESULTS")
            if result:
                ingested_items = result.get('ingested', [])
                updated_items = result.get('updated', [])
                errors = result.get('errors', [])
                summary = result.get('summary', [])
            # TODO: return a results object from ingest process
            # ...and log an error if you don't get it
            # else:
            #     err = "No return value from ingest script dlp_ingest_main()"
            #     logger.error(err)

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
                    log_lines=log_lines,
                    ingest_config=ingestConfig
                )
        else:
            err = "Missing required metadata file for selected ingest type"
            logger.error(err)
    else:
        logger.info("/submit received GET. Redirecting home")
   
    return redirect(url_for("index", msg="There was an exception in the process. Please check the logs. ...my bad"))