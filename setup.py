__version__ = '1.0.0-dev'

setup_args = {
	'name': 'paperdata',
	'author': 'Immanuel Washington',
	'author_email': 'immwa at sas.upenn.edu',
	'license': '?',
	#'package_dir' : {'paperdata': ''},
	'packages' : ['paperdata', 'paperdata.data', 'paperdata.ganglia', 'paperdata.dev', 'paperdata.convert'],
	'version': __version__,
}

if __name__ == '__main__':
	from distutils.core import setup
	try:
		apply(setup, (), setup_args)
	except:
		setup(**setup_args)
