import glob

__version__ = '1.0'

setup_args = {
	'name': 'convert',
	'author': 'Immanuel Washington',
	'author_email': 'immwa at sas.upenn.edu',
	'license': 'GPL',
	'package_dir' : {'convert': ''},
	'packages' : ['convert'],
	'version': __version__,
}

if __name__ == '__main__':
	from distutils.core import setup
	apply(setup, (), setup_args)
