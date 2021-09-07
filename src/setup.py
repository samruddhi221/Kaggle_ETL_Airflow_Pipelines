from setuptools import setup, find_packages
import os
import sys

if sys.version_info < (3, 6):
    sys.exit("Python version less than  3.6 not supported.")

REQUIRED = ['pandas', 'numpy', 'sklearn']

DEPENDENCY_LINKS = []

setup(name='kaggle_etl',
      description='library for kaggle ETL utilities',
      packages=[package for package in find_packages()],
      insttall_required=REQUIRED,
      dependency_links=DEPENDENCY_LINKS)
