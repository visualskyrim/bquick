# -*- coding: utf-8 -*-

import sys
import os
import copy
from collections import namedtuple
from multiprocessing import Pool
from bquick.config_parser import get_config
from bquick.bigquery import table_list_handler

BQ_CONFIG = get_config()

DeleteProcessParam = namedtuple("DeleteProcessParam", "bq_client \
                                                       dataset \
                                                       table_name")


def delete_table_by_name(bq_client, dataset, table_name, ignore_confirm=False):
  """Delete the table with the given name.
  """
  _delete_table_list(bq_client, dataset, [table_name], ignore_confirm)


def delete_table_using_file(bq_client, dataset, delete_file_path,
                            ignore_confirm=False):
  """Delete all the tables in the given file.
  """
  if not os.path.exists(delete_file_path):
    raise ValueError("Given file path doesn't exist: %s" % arg_file_path)

  with open(delete_file_path) as table_list_file:
    table_list = [line.rstrip() for line in table_list_file.readlines()]
    _delete_table_list(bq_client, dataset, table_list, ignore_confirm)


def delete_table_with_wildcard(bq_client,
                               dataset,
                               table_prefix,
                               start_date,
                               end_date,
                               ignore_confirm=False):
  """Delete all the tables matching the wildcard table prefix.
  """
  table_list = table_list_handler.list_wildcard_table(bq_client,
                                                      dataset,
                                                      table_prefix,
                                                      start_date,
                                                      end_date,
                                                      sys.maxint)
  _delete_table_list(bq_client, dataset, table_list, ignore_confirm)


def delete_table_with_regex(bq_client, dataset, table_name_pattern,
                            ignore_confirm=False):
  """Delete all the tables that match given regex pattern.
  """
  table_list = table_list_handler.list_regex_table(bq_client,
                                                   dataset,
                                                   table_name_pattern,
                                                   sys.maxint)
  _delete_table_list(bq_client, dataset, table_list, ignore_confirm)


def _delete_table_list(bq_client, dataset, table_name_list, ignore_confirm):
  """Delete all the tables in the list
  """
  if not ignore_confirm:
    if not __confirm_delete(dataset, table_name_list):
      return

  # TODO: make parallel happen
#  delete_process_pool = Pool(processes=2)
#  delete_process_pool.map(
#      __delete_table_process,
#      [DeleteProcessParam(bq_client=copy.deepcopy(bq_client),
#                          dataset=dataset,
#                          table_name=table_name) \
#                          for table_name in table_name_list])
#  delete_process_pool.close()
#  delete_process_pool.join()
  for table_name in table_name_list:
    param = DeleteProcessParam(bq_client=bq_client,
                               dataset=dataset,
                               table_name=table_name)
    __delete_table_process(param)


def __confirm_delete(dataset, table_name_list):
  """This function will block process and ask for confirmation.
  All the table names will be shown.
  """
  proceed_choices = ['yes', 'y']
  abort_choices = ['no', 'n']

  for table_name in table_name_list:
    print table_name

  print "Delete all these tables from dataset [%s]? [y or n]" % dataset

  while True:
    choice = raw_input().lower()
    if choice in proceed_choices:
      return True
    elif choice in abort_choices:
      return False
    else:
      print "Please enter [y or n]"


def __delete_table_process(param):
  """Delete table with compressed param:
  Args:
    - param: A dict consists of:
      - bq_client: a bigquery service object.
      - dataset: the dataset to operate on.
      - table_name: of which table to be deleted.
  """
  bq_client = param.bq_client
  dataset = param.dataset
  table_name = param.table_name

  __delete_table(bq_client, dataset, table_name)


def __delete_table(bq_client, dataset, table_name):
  """Delete the table with given name.
  """
  try:
    bq_client.tables() \
        .delete(projectId=BQ_CONFIG.project,
                datasetId=dataset,
                tableId=table_name) \
        .execute()
  except Exception as e:
    # TODO: Logging
    raise
