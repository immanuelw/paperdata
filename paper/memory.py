from __future__ import print_function
import psutil
import smtplib

def enough_memory(required_memory, memory_path):
	'''
	checks path for enough memory

	Parameters
	----------
	required_memory | int: amount of memory needed in bytes
	memory_path | str: path to check for space

	Returns
	-------
	bool: is there enough memory
	'''
	free_memory = getattr(psutil.disk_usage(memory_path), 'free')

	return required_memory < free_memory

def email_memory(table):
	'''
	emails people if there is not enough memory on folio

	Parameters
	----------
	table | str: table name
	'''
	start_email = 'paperfeed.paperdata@gmail.com'
	start_pass = 'papercomesfrom1tree'

	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login(start_email, start_pass)

	#Send the mail
	header = 'From: PAPERBridge <paperfeed.paper@gmail.com>\nSubject: NOT ENOUGH SPACE ON FOLIO\n'
	msgs = ''.join((header, '\nNot enough memory for ', table, ' on folio'))

	emails = ('immwa@sas.upenn.edu', 'jaguirre@sas.upenn.edu', 'saul.aryeh.kohn@gmail.com', 'jacobsda@sas.upenn.edu')
	for user in emails:
		server.sendmail(start_email, user, msgs)

	server.quit()
if __name__ == '__main__':
	print('This is a module')
