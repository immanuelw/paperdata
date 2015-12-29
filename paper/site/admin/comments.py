'''
paper.site.admin.comments

author | Immanuel Washington

Functions
---------
get_all_comments | displays all comments in database in descending order
thread_reply | adds thread reply to database
new_thread | creates new thread and adds to database
'''
from flask import render_template, g, make_response, request
from paper.site.flask_app import admin_app as app, admin_db as db
from paper.site.admin import models
from paper.site import db_utils
from datetime import datetime

@app.route('/get_all_comments')
def get_all_comments():
    '''
    get all comments

    Returns
    -------
    html: comments
    '''
    try:
        threads = db_utils.query(database='admin', table='Thread', sort_tuples=(('last_updated', 'desc'),))
        for thread in threads:
            thread.comments = db_utils.query(database='admin', table='Comment', field_tuples=(('thread_id', '==', thread.id),))
    except:
        return make_response('Threads not found', 500)

    return render_template('comments_list.html', threads=threads)

@app.route('/thread_reply', methods = ['POST'])
def thread_reply():
    '''
    add thread reply
    '''
    if g.user is not None and g.user.is_authenticated():
        thread_id = request.form['thread_id']
        text = request.form['text']

        new_comment = models.Comment(thread_id=thread_id, text=text, username=g.user.username)
        db.session.add(new_comment)

        thread = db_utils.query(database='admin', table='Thread', field_tuples=(('id', '==', thread_id),))
        thread.last_updated = datetime.utcnow()
        db.session.add(thread)
        db.session.commit()

        return make_response('Success', 200)
    else:
        return make_response('You need to be logged in to post a comment.', 401)

@app.route('/new_thread', methods = ['POST'])
def new_thread():
    '''
    add new comment thread
    '''
    if g.user is not None and g.user.is_authenticated():
        title = request.form['title']
        text = request.form['text']

        new_thread = models.Thread(title=title, username=g.user.username)
        db.session.add(new_thread)
        db.session.flush()
        db.session.refresh(new_thread) # So we can get the new thread's id

        first_comment = models.Comment(text=text, username=g.user.username, thread_id=new_thread.id)
        db.session.add(first_comment)
        db.session.commit()

        return make_response('Success', 200)
    else:
        return make_response('You need to be logged in to create a thread.', 401)
