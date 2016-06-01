from setuptools import find_packages
__version__ = '1.0.1-dev'

setup_args = {
    'name': 'paperdata',
    'author': 'Immanuel Washington',
    'author_email': 'immwa at sas.upenn.edu',
    'description': 'package for maintaining the PAPER project',
    'url': 'https://github.com/immanuelw/paperdata',
    'license': '?',
    'package_dir' : {'paperdata': ''},
    'packages' : find_packages(),
    'version': __version__,
}

if __name__ == '__main__':
    try:
        from setuptools import setup
    except:
        from distutils.core import setup
    try:
        apply(setup, (), setup_args)
    except:
        setup(**setup_args)
