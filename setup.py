#!/usr/bin/env python3
from setuptools import setup

from pysonic import __version__

setup(name='pysonic',
      version=__version__,
      description='pysonic audio server',
      url='http://gitlab.davepedu.com/dave/pysonic',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['pysonic'],
      entry_points={'console_scripts': ['pysonicd=pysonic.daemon:main']})
