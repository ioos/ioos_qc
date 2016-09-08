import os
from setuptools import setup, find_packages


def extract_version(module='ioos_qartod'):
    version = None
    fdir = os.path.dirname(__file__)
    fnme = os.path.join(fdir, module, '__init__.py')
    with open(fnme) as fd:
        for line in fd:
            if (line.startswith('__version__')):
                _, version = line.split('=')
                # Remove quotation characters.
                version = version.strip()[1:-1]
                break
    return version

rootpath = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    return open(os.path.join(rootpath, *parts), 'r').read()

with open('requirements.txt') as f:
    require = f.readlines()
install_requires = [r.strip() for r in require]
long_description = '{}\n'.format(read('README.rst'))
LICENSE = read('LICENSE.txt')

setup(name='ioos_qartod',
      version=extract_version(),
      author='Ben Adams',
      author_email='BAdams@asascience.com',
      packages=find_packages(),
      install_requires=install_requires,
      license=LICENSE,
      url='https://github.com/ioos/qartod',
      description='IOOS QARTOD Tests implemented in Python',
      long_description=long_description,
      classifiers=['Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                   'Topic :: Scientific/Engineering',
                   'Topic :: Scientific/Engineering :: GIS',
                   'Topic :: Scientific/Engineering :: Information Analysis',
                   'Topic :: Scientific/Engineering :: Mathematics',
                   'Topic :: Scientific/Engineering :: Physics']
      )
