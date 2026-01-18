import os
from flask import redirect, render_template, request, session, url_for

from ingest import main as dlp_ingest_main
import utils.web_utils as utils


def index():
    user = session.get('user')
    if user:
        return  redirect(url_for("ingest_form"))
    else:
        return render_template("login_page.html")
    

def ingest_form():
    user = session.get('user')
    return render_template("form_full.html", user=user)


def submit(application):
    ingestConfig = {}

    uploaded = []
    ingested_items = []
    updated_items = []
    errors = []
    summary = []

    ingestConfig = utils.set_environment_defaults(application, ingestConfig)
    collection_identifier = utils.get_identifier()
    if request.method == 'POST' and 'metadata_input' in request.files:
        uploaded = utils.save_uploads(application, collection_identifier, len(request.files.getlist('metadata_input')))
    

    if utils.files_exist(application):
        ingestConfig = utils.set_environment_overrides(application, ingestConfig)

        # Do the ingest
        metadata_filepath = os.path.join(application.config['UPLOADS'], uploaded[0])

        result = None
        result = dlp_ingest_main(None, None, metadata_filepath, ingestConfig)
        if result:
            print(f"DEBUG: Result returned by dlp_ingest_main: {result}")
            ingested_items = result.get('ingested', [])
            updated_items = result.get('updated', [])
            errors = result.get('errors', [])
            summary = result.get('summary', [])
            print(f"DEBUG: ingested_items: {ingested_items}")
            print(f"DEBUG: updated_items: {updated_items}")
            print(f"DEBUG: errors: {errors}")
            print(f"DEBUG: summary: {summary}")

        # Write files for download
        results_dir = os.path.join(application.config['APPLICATION_ROOT'], 'results')
        os.makedirs(results_dir, exist_ok=True)

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

        
    return render_template(
        'submit.html',
        ingested_count=len(ingested_items),
        updated_count=len(updated_items),
        errors_count=len(errors),
        summary_count=len(summary)
    )