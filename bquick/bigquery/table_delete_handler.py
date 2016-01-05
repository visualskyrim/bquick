# -*- coding: utf-8 -*-

import sys
import os
from collections import namedtuple
from multiprocessing import Pool

#from bquick.bigquery import table_list_handler, \
#                            GOOGLE_BIGQUERY_CLIENT, \
#                            BQ_CONFIG

DeleteProcessParam = namedtuple("DeleteProcessParam", "bq_client \
                                                       dataset \
                                                       table_name")

def delete_table_by_name(bq_client, dataset, table_name):
  """Delete the table with the given name.
  """
  __delete_table(bq_client, dataset, table_name)

def delete_table_using_file(bq_client, dataset, file_path):
  """Delete all the tables in the given file.
  """
  arg_file_path = del_command.delete_file
  if os.path.isabs(arg_file_path):
    delete_file_path = arg_file_path
  else:
    working_path = os.getcwd()
    delete_file_path = os.path.join(working_path, arg_file_path)

  if not os.path.exists(delete_file_path):
    raise ValueError("Given file path doesn't exist: %s" % arg_file_path)

  with open(delete_file_path) as table_list_file:
    table_list = table_list_file.readlines()
    __delete_table_list(bq_client, dataset, table_list)

def delete_table_with_wildcard(bq_client,
                               dataset,
                               table_prefix,
                               start_date,
                               end_date):
  """Delete all the tables matching the wildcard table prefix.
  """
  table_list = table_list_handler.list_wildcard_table(bq_client,
                                                      dataset,
                                                      table_prefix,
                                                      start_date,
                                                      end_date)
  __delete_table_list(dataset, table_list)

def delete_table_with_regex(bq_client, dataset, table_name_pattern):
  """Delete all the tables that match given regex pattern.
  """
  table_list = table_list_handler.list_regex_table(bq_client,
                                                   dataset,
                                                   table_name_pattern,
                                                   sys.maxint)
  __delete_table_list(dataset, table_list)


def __delete_table_list(bq_client, dataset, table_name_list):
  """Delete all the tables in the list
  """
  # TODO: show the tables to be deleted, and ask whether to delete.
  delete_process_pool = Pool()
  delete_process_pool.map(
      __delete_table, [DeleteProcessParam(bq_client=bq_client,
                                          dataset=dataset,
                                          table_name=table_name) \
                                          for table_name in table_name_list])
  delete_process_pool.close()
  delete_process_pool.join()

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
