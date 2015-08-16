from flask import render_template, flash, redirect, url_for, request, g
from flask.ext.login import login_user, logout_user
from app.flask_app import app, lm, db
from app import models
import hashlib
import re

@lm.user_loader
def load_user(id):
	return models.User.query.get(id)

@app.route('/login', methods = ['GET', 'POST'])
def login():
	if g.user is not None and g.user.is_authenticated():
		return redirect(url_for('index'))
	error = None
	if request.method == 'POST':
		username = request.form['username'].strip()
		password = request.form['password'].strip()

		u = models.User.query.get(username)
		password = password.encode('UTF-8')
		if not u:
			error = 'Invalid username/password combination.'
		elif u.password != hashlib.sha512(password).hexdigest():
			error = 'Invalid username/password combination.'
		else:
			login_user(u)
			flash('You were logged in', 'flash')
			return redirect(url_for('index'))
	return render_template('login.html', error=error)

@app.route('/signup', methods= ['GET', 'POST'])
def signup():
	if g.user is not None and g.user.is_authenticated():
		return redirect(url_for('index'))
	error = None
	if request.method == 'POST':
		username = request.form['username'].strip()
		password = request.form['password'].strip()
		password2 = request.form['password2'].strip()
		email = request.form['email'].strip()
		fname = request.form['fname'].strip()
		lname = request.form['lname'].strip()

		testU = models.User.query.get(username)

		if password != password2:
			error = "Passwords must be the same."
		elif testU is not None:
			error = "That username is already in use."
		elif not re.match(r"[^@]+@[^@]+\.[^@]+", email):
			error = "That email address is not correct."
		else:
			real_pass = password.encode('UTF-8')

			new_user = models.User(username, hashlib.sha512(real_pass).hexdigest(), email, fname, lname)
			db.session.add(new_user)
			db.session.flush()
			db.session.refresh(new_user)
			db.session.commit()

			u = models.User.query.get(username)
			login_user(u)
			flash('You were logged in', 'flash')
			return redirect(url_for('index'))
	return render_template('signup.html', error=error)

@app.route('/logout')
def logout():
	logout_user()
	flash('You were logged out', 'flash')
	return redirect(url_for('index'))

@app.route('/delete_user', methods=['POST'])
def delete_user():
	if (g.user is not None and g.user.is_authenticated()):
		username = request.form['username']
		action = request.form['action']

		setList = models.Set.query.filter(models.Set.username == username)

		for aSet in setList:
			theSet = models.Set.query.filter(models.Set.id == aSet.id).first()
			if action == 'transfer':
				theSet.username = g.user.username
			else: #destroy, cascade deletion
				db.session.delete(theSet)
			db.session.commit()

		u = models.User.query.filter(models.User.username == username).first()

		db.session.delete(u)
		db.session.commit()

		return redirect(url_for('user_page'))
