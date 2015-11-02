############################################################
# Dockerfile to build Python WSGI Application Containers
# Based on Ubuntu
############################################################

# Set the base image to Ubuntu
FROM ubuntu
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# File Author / Maintainer
MAINTAINER Immanuel Washington

# Add the application resources URL
RUN echo "deb http://archive.ubuntu.com/ubuntu/ $(lsb_release -sc) main universe" >> /etc/apt/sources.list

# Update the sources list
RUN apt-get update

# Install basic applications
RUN apt-get install -y tar git curl vim wget dialog net-tools build-essential

# Install Python and Basic Python Tools
RUN apt-get install -y -o APT::Install-Recommends=false -o APT::Install-Suggests=false \
	python-pip python-virtualenv python3-dev libpq-dev postgresql libmysqlclient-dev mysql-client \
	autoconf libtool pkg-config python-opengl python-imaging python-pyrex python-pyside.qtopengl \
	idle-python2.7 qt4-dev-tools qt4-designer libqtgui4 libqtcore4 libqt4-xml libqt4-test libqt4-script \
	libqt4-network libqt4-dbus python-qt4 python-qt4-gl libgle3 python-dev

# Sync the application folder inside the container
VOLUME ~/paperdata /paperdata

# Get pip to download and install requirements:
RUN pip install --upgrade virtualenv
RUN virtualenv --python=/usr/bin/python3.4 paperenv
RUN source paperenv/bin/activate
RUN pip install alembic==0.8.2 \
	ecdsa==0.13 \
	Flask==0.10.1 \
	Flask-Login==0.2.11 \
	Flask-Migrate==1.5.1 \
	Flask-Script==2.0.5 \
	Flask-SQLAlchemy==2.0 \
	itsdangerous==0.24 \
	Jinja2==2.8 \
	Mako==1.0.2 \
	MarkupSafe==0.23 \
	mysqlclient==1.3.6 \
	numpy==1.9.2 \
	paramiko==1.15.2 \
	prettytable==0.7.2 \
	psycopg2==2.6.1 \
	pycrypto==2.6.1 \
	python-editor==0.4 \
	requests==2.7.0 \
	requests-futures==0.9.5 \
	SQLAlchemy==1.0.8 \
	Werkzeug==0.10.4 \
	wheel==0.24.0
#RUN pip install -r /paperdata/requirements.txt

# Expose ports
EXPOSE 80

ENTRYPOINT /bin/bash

#access database
#RUN sudo -u postgres createuser immwa

# Set the default directory where CMD will execute
#WORKDIR /paperdata
#RUN python setup.py develop

# Set the default command to execute    
# when creating a new container
#CMD python /paperdata/paper/site/search/scripts/run_app.py
