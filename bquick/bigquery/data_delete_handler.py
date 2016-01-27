# -*- coding: utf-8 -*-

import sys
import os
import uuid
import copy
from collections import namedtuple
from multiprocessing import Pool
from bquick.config_parser import get_config
from bquick.bigquery import table_list_handler, table_delete_handler, table_copy_handler, utils

BQ_CONFIG = get_config()

SELECT_REMAIN_QUERY = """
SELECT * FROM %s.%s WHERE NOT (%s);
"""


def delete_data_by_name(bq_client, dataset, condition, table_name):
  __delete_data_process(bq_client, dataset, condition, [table_name])


def delete_data_using_file(bq_client, dataset, condition, delete_file_path):
  """Delete all the tables in the given file.
  """
  if not os.path.exists(delete_file_path):
    raise ValueError("Given file path doesn't exist: %s" % arg_file_path)

  with open(delete_file_path) as table_list_file:
    table_list = [line.rstrip() for line in table_list_file.readlines()]
    __delete_data_process(bq_client, dataset, condition, table_list)


def delete_data_with_wildcard(bq_client,
                              dataset,
                              condition,
                              table_prefix,
                              start_date,
                              end_date):
  """Delete all the tables matching the wildcard table prefix.
  """
  table_list = table_list_handler.list_wildcard_table(bq_client,
                                                      dataset,
                                                      table_prefix,
                                                      start_date,
                                                      end_date,
                                                      sys.maxint)
  __delete_data_process(bq_client, dataset, condition, table_list)


def delete_data_with_regex(bq_client, dataset, condition, table_name_pattern):
  """Delete all the tables that match given regex pattern.
  """
  table_list = table_list_handler.list_regex_table(bq_client,
                                                   dataset,
                                                   table_name_pattern,
                                                   sys.maxint)
  __delete_data_process(
      bq_client, dataset, condition, table_list)


def __delete_data_process(bq_client, dataset, condition, table_name_list):

  # create temp dataset
  temp_dataset_name = __create_temp_dataset(bq_client)

  # get data we need to temp dataset with the same table name
  job_table_map = {}

  print "Create tables with data remain."
  table_package_list = utils.get_table_packages(table_name_list)
  for table_name_package in table_package_list:
    for table_name in table_name_package:
      job_id = __query_out_remain_data(
          bq_client, table_name, condition, dataset, temp_dataset_name)
      job_table_map[job_id] = table_name

    utils.thruhold_jobs(bq_client)

  # wait job to finish
  print "Wait for all temp tables are created."
  utils.wait_all_job_finish(bq_client, job_table_map.keys())

  # TODO: validate the temp data

  # delete the origin table
  print "Delete origin tables."
  table_delete_handler._delete_table_list(
      bq_client, dataset, table_name_list, ignore_confirm=True)

  # copy table with the data we want to the origin dataset
  print "Copy the remaining data."
  table_copy_handler._copy_table_list(
      bq_client, temp_dataset_name, dataset, table_name_list, ignore_confirm=True)

  # TODO: delete the temp dataset


def __create_temp_dataset(bq_client):
  project_id = BQ_CONFIG.project
  temp_dataset_name = "bquick_%s" % uuid.uuid4().time_low
  print "Create temp dataset: %s" % temp_dataset_name

  job_body = {
      "datasetReference": {
          "projectId": project_id,
          "datasetId": temp_dataset_name
      }
  }

  bq_client.datasets().insert(projectId=project_id, body=job_body).execute()

  return temp_dataset_name


def __query_out_remain_data(bq_client, table_name, condition, origin_dataset_name, temp_dataset_name):
  print "Save remaining data to %s:%s" % (temp_dataset_name, table_name)

  project_id = BQ_CONFIG.project
  select_remain_query_content = SELECT_REMAIN_QUERY % (
      origin_dataset_name, table_name, condition)
  return utils.query_into(
      bq_client, table_name, table_name, select_remain_query_content, origin_dataset_name, temp_dataset_name)


def __delete_temp_dataset(bq_client, temp_dataset_name):
  pass
