'''
paper.site.admin.users

author | Immanuel Washington

Functions
---------
load_user | loads user
login | logs into website
signup | adds user to website
logout | logs out of website
delete_user | deletes user from database/website
'''
from flask import render_template, flash, redirect, url_for, request, g
from flask.ext.login import login_user, logout_user
from paper.site.flask_app import admin_app as app, admin_db as db, admin_lm as lm
from paper.site.admin import models
from paper.site import db_utils
import hashlib
import re

@lm.user_loader
def load_user(id):
    '''
    load user

    Parameters
    ----------
    id | int: user id

    Returns
    -------
    object: user object
    '''
    user = db_utils.query(database='admin', table='User', field_tuples=(('username', '==', id),))[0]

    return user

@app.route('/login', methods = ['GET', 'POST'])
def login():
    '''
    login

    Returns
    -------
    html: login
    '''
    if g.user is not None and g.user.is_authenticated():
        return redirect(url_for('index'))
    error = None
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password'].strip()

        u = db_utils.query(database='admin', table='User', field_tuples=(('username', '==', username),))[0]

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
    '''
    signup

    Returns
    -------
    html: signup
    '''
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

        testU = db_utils.query(database='admin', table='User', field_tuples=(('username', '==', username),))[0]

        if password != password2:
            error = 'Passwords must be the same.'
        elif testU is not None:
            error = 'That username is already in use.'
        elif not re.match(r'[^@]+@[^@]+\.[^@]+', email):
            error = 'That email address is not correct.'
        else:
            real_pass = password.encode('UTF-8')

            new_user = models.User(username=username, password=hashlib.sha512(real_pass).hexdigest(),
                                    email=email, firstname=fname, lastname=lname)
            db.session.add(new_user)
            db.session.flush()
            db.session.refresh(new_user)
            db.session.commit()

            u = db_utils.query(database='admin', table='User', field_tuples=(('username', '==', username),))[0]

            login_user(u)
            flash('You were logged in', 'flash')
            return redirect(url_for('index'))

    return render_template('signup.html', error=error)

@app.route('/logout')
def logout():
    '''
    logout

    Returns
    -------
    html: redirect to index
    '''
    logout_user()
    flash('You were logged out', 'flash')

    return redirect(url_for('index'))

@app.route('/delete_user', methods=['POST'])
def delete_user():
    '''
    delete user from database and delete all sets associated to user

    Returns
    -------
    html: redirect to user page
    '''
    if (g.user is not None and g.user.is_authenticated()):
        username = request.form['username']
        action = request.form['action']

        field_tuple_base = (('username', '==', username),)

        for aSet in setList:
            field_tuples = field_tuple_base + (('id', '==', aSet.id),)

            theSet = db_utils.query(database='admin', table='Set', field_tuples=field_tuples)[0]

            if action == 'transfer':
                theSet.username = g.user.username
            else: #destroy, cascade deletion
                db.session.delete(theSet)
            db.session.commit()

        u = db_utils.query(database='admin', table='User', field_tuples=(('username', '==', username),))[0]
        db.session.delete(u)
        db.session.commit()

        return redirect(url_for('user_page'))
