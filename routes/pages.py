from flask import redirect, render_template, session, url_for


def index():
    user = session.get('user')
    if user:
        return  redirect(url_for("ingest_form"))
    else:
        return render_template("login_page.html")
    

def ingest_form():
    user = session.get('user')
    return render_template("form.html", user=user)