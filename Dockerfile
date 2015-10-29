############################################################
# Dockerfile to build Python WSGI Application Containers
# Based on Ubuntu
############################################################

# Set the base image to Ubuntu
FROM ubuntu

# File Author / Maintainer
MAINTAINER Immanuel Washington

# Add the application resources URL
RUN echo "deb http://archive.ubuntu.com/ubuntu/ $(lsb_release -sc) main universe" >> /etc/apt/sources.list

# Update the sources list
RUN apt-get update

# Install basic applications
RUN apt-get install -y tar git curl vim wget dialog net-tools build-essential

# Install Python and Basic Python Tools
RUN apt-get install -y python-virtualenv python3-dev libpq-dev postgresql libmysqlclient-dev mysql-client

# Sync the application folder inside the container
VOLUME ~/paperdata /paperdata

# Get pip to download and install requirements:
RUN pip install --upgrade virtualenv
RUN virtualenv --python=/usr/bin/python3.4 paperenv
RUN source paperenv/bin/activate
RUN pip install -r /paperdata/requirements.txt

# Expose ports
EXPOSE 80

#access database
RUN sudo -u postgres createuser vagrant

# Set the default directory where CMD will execute
WORKDIR /paperdata
RUN python setup.py develop

# Set the default command to execute    
# when creating a new container
CMD python /paperdata/paper/site/search/scripts/run_app.py
