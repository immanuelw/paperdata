import glob

__version__ = '0.0.1'

setup_args = {
	'name': 'paperdataDB',
	'author': 'Immanuel Washington',
	'author_email': 'immwa at sas.upenn.edu',
	'license': 'GPL',
	'package_dir' : {'paperdataDB': ''},
	'packages' : ['paperdataDB'],
	'scripts': glob.glob('scripts/*'),
	'version': __version__,
}

if __name__ == '__main__':
	from distutils.core import setup
	apply(setup, (), setup_args)
