# -*- coding: utf-8 -*-
from setuptools import setup

setup(name='bquick',
      version='0.1.7',
      description='The command line tool for BigQuery management.',
      url='http://github.com/visualskyrim/bquick',
      author='visualskyrim',
      author_email='visual.skyrim@gmail.com',
      license='MIT',
      packages=['bquick', 'bquick.bigquery'],
      install_requires=[
          'google-api-python-client',
          'oauth2client'
      ],
      scripts=['bin/bquick'],
      zip_safe=False)
