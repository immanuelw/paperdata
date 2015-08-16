from flask import render_template, g, make_response, request
from app.flask_app import app, db
from app import models
from datetime import datetime

@app.route('/get_all_comments')
def get_all_comments():
    threads = models.Thread.query.order_by(models.Thread.last_updated.desc()).all()

    for thread in threads:
        thread.comments = models.Comment.query.filter(models.Comment.thread_id == thread.id).all()

    return render_template('comments_list.html', threads=threads)

@app.route('/thread_reply', methods = ['POST'])
def thread_reply():
    if g.user is not None and g.user.is_authenticated():
        thread_id = request.form['thread_id']
        text = request.form['text']

        new_comment = models.Comment()
        new_comment.thread_id = thread_id
        new_comment.text = text
        new_comment.username = g.user.username
        db.session.add(new_comment)

        thread = models.Thread.query.filter(models.Thread.id == thread_id).first()
        thread.last_updated = datetime.utcnow()
        db.session.add(thread)

        db.session.commit()
        return make_response("Success", 200)
    else:
        return make_response("You need to be logged in to post a comment.", 401)

@app.route('/new_thread', methods = ['POST'])
def new_thread():
    if g.user is not None and g.user.is_authenticated():
        title = request.form['title']
        text = request.form['text']

        new_thread = models.Thread()
        new_thread.title = title
        new_thread.username = g.user.username
        db.session.add(new_thread)
        db.session.flush()
        db.session.refresh(new_thread) # So we can get the new thread's id

        first_comment = models.Comment()
        first_comment.text = text
        first_comment.username = g.user.username
        first_comment.thread_id = new_thread.id
        db.session.add(first_comment)
        db.session.commit()
        return make_response("Success", 200)
    else:
        return make_response("You need to be logged in to create a thread.", 401)
