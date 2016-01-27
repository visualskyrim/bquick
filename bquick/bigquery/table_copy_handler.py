# -*- coding: utf-8 -*-

import sys
import os
import copy
import time
from collections import namedtuple
from multiprocessing import Pool
from bquick.config_parser import get_config
from bquick.bigquery import table_list_handler, utils

BQ_CONFIG = get_config()


def copy_table_file(bq_client, dataset, dest, file_path):
  """Copy table with the name in a specific file.
  """
  if not os.path.exists(file_path):
    raise ValueError("Given file path doesn't exist: %s" % arg_file_path)

  with open(file_path) as table_list_file:
    table_list = [line.rstrip() for line in table_list_file.readlines()]
  _copy_table_list(bq_client, dataset, dest, table_list)


def copy_table_regex(bq_client, dataset, dest, table_name_pattern):
  """Copy the tables with the name that matches certern regex pattern.
  """
  table_list = table_list_handler.list_regex_table(
      bq_client,
      dataset,
      table_name_pattern,
      sys.maxint)
  _copy_table_list(bq_client, dataset, dest, table_list)


def copy_table_wildcard(
        bq_client, dataset, dest, table_prefix, start_date, end_date):
  """Copy the tables with name match given wildcard pattern.
  """
  table_list = table_list_handler.list_wildcard_table(
      bq_client,
      dataset,
      table_prefix,
      start_date,
      end_date,
      sys.maxint)
  _copy_table_list(bq_client, dataset, dest, table_list)


def _copy_table_list(
        bq_client,
        org_dataset,
        dest_dataset,
        table_name_list,
        ignore_confirm=False):
  # ask for confirmation
  if not ignore_confirm:
    print ""
    for table_name in table_name_list:
      print table_name
    print ""
    print "The [%d] tables above is going to be copied." \
        % len(table_name_list)
    print "From dataset: [%s]" % org_dataset
    print "To   dataset: [%s]" % dest_dataset
    print ""
    print "Is it ok? [y/N]"

    proceed_choices = ['yes', 'y']
    abort_choices = ['no', 'n']

    while True:
      choice = raw_input().lower()
      if choice in proceed_choices:
        break
      if choice in abort_choices:
        return
      else:
        print "Please enter [y or n]"

  # get launch packages
  package_list = utils.get_table_packages(table_name_list)

  for sub_table_name_list in package_list:
    # check running jobs
    utils.thruhold_jobs(bq_client)

    for table_name in sub_table_name_list:
      __copy_table(
          bq_client,
          org_dataset,
          dest_dataset,
          table_name,
          table_name)


def __copy_table(
        bq_client, org_dataset, dest_dataset, table_name, dest_table_name):
  job_data = {
      'configuration': {
          "copy": {
              "writeDisposition": "WRITE_APPEND",
              "sourceTable": {
                  "projectId": BQ_CONFIG.project,
                  "tableId": table_name,
                  "datasetId": org_dataset
              },
              "destinationTable": {
                  "projectId": BQ_CONFIG.project,
                  "tableId": dest_table_name,
                  "datasetId": dest_dataset
              }
          }
      }
  }
  resp = bq_client.jobs() \
      .insert(projectId=BQ_CONFIG.project, body=job_data) \
      .execute(num_retries=10)
  return resp
