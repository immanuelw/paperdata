from flask import render_template, g, make_response, request
from paper.site.admin.flask_app import app, db
from paper.site.admin import models
from paper.site import db_utils
from datetime import datetime

@app.route('/get_all_comments')
def get_all_comments():
	try:
		threads = db_utils.query(database='eorlive', table='thread', sort_tuples=(('last_updated', 'desc'),))
		for thread in threads:
			setattr(thread, 'comments',
					db_utils.query(database='eorlive', table='comment',	field_tuples=(('thread_id', '==', getattr(thread, 'id')),)))
	except:
		return make_response('Threads not found', 500)

	return render_template('comments_list.html', threads=threads)

@app.route('/thread_reply', methods = ['POST'])
def thread_reply():
	if g.user is not None and g.user.is_authenticated():
		thread_id = request.form['thread_id']
		text = request.form['text']

		new_comment = getattr(models, 'Comment')()
		setattr(new_comment, 'thread_id', thread_id)
		setattr(new_comment, 'text', text)
		setattr(new_comment, 'username', g.user.username)

		db.session.add(new_comment)

		thread = db_utils.query(database='eorlive', table='thread',	field_tuples=(('id', '==', thread_id),))
		setattr(thread, 'last_updated', datetime.utcnow())

		db.session.add(thread)
		db.session.commit()
		return make_response('Success', 200)
	else:
		return make_response('You need to be logged in to post a comment.', 401)

@app.route('/new_thread', methods = ['POST'])
def new_thread():
	if g.user is not None and g.user.is_authenticated():
		title = request.form['title']
		text = request.form['text']

		new_thread = getattr(models, 'Thread')()
		setattr(new_thread, 'title', title)
		setattr(new_thread, 'username', g.user.username)

		db.session.add(new_thread)
		db.session.flush()
		db.session.refresh(new_thread) # So we can get the new thread's id

		first_comment = getattr(models, 'Comment')()
		setattr(first_comment, 'text', text)
		setattr(first_comment, 'username', g.user.username)
		setattr(first_comment, 'thread_id', getattr(new_thread, 'id'))

		db.session.add(first_comment)
		db.session.commit()

		return make_response('Success', 200)
	else:
		return make_response('You need to be logged in to create a thread.', 401)
