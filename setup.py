from setuptools import setup, find_packages
from ioos_qartod import __version__

packages = find_packages()

with open('requirements.txt') as f:
    reqs = [line.strip() for line in f]
with open('README.md') as f:
    readme = f.read()

setup(name='ioos_qartod',
      version=__version__,
      author='Ben Adams',
      author_email='BAdams@asascience.com',
      packages=find_packages(),
      install_requires=reqs,
      license='Apache 2.0',
      url='https://github.com/asascience-open/QARTOD',
      description='IOOS QARTOD Tests implemented in Python',
      long_description=readme,
      classifiers=['Programming Language :: Python :: 2.7',
                   'Topic :: Scientific/Engineering',
                   'Topic :: Scientific/Engineering :: GIS',
                   'Topic :: Scientific/Engineering :: Information Analysis',
                   'Topic :: Scientific/Engineering :: Mathematics',
                   'Topic :: Scientific/Engineering :: Physics']
      )
