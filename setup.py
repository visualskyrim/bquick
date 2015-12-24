# -*- coding: utf-8 -*-
from setuptools import setup

setup(name='bquick',
      version='0.1',
      description='The command line tool for BigQuery management.',
      url='http://github.com/visualskyrim/bquick',
      author='visualskyrim',
      author_email='visual.skyrim@gmail.com',
      license='MIT',
      packages=['bquick', 'bquick.bigquery'],
      install_requires=[
      ],
      scripts=['bin/bquick'],
      zip_safe=False)
