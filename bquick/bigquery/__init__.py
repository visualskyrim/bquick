# -*- coding: utf-8 -*-

import httplib2
import urllib
import uuid
import time
import os
import json
import sys
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials
from urllib2 import HTTPError

from bquick.bigquery import table_list_handler, table_delete_handler
from bquick.command_parser import ListCommand, \
                                  ListRegexCommand, \
                                  ListWildcardCommand, \
                                  DeleteFileCommand, \
                                  DeleteNamedCommand, \
                                  DeleteRegexCommand, \
                                  DeleteWildcardCommand


# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

def list_table(list_command):
  dataset = list_command.dataset
  limit = list_command.limit

  if isinstance(list_command, ListCommand):
    return table_list_handler.list_all_table(GOOGLE_BIGQUERY_CLIENT,
                                             dataset,
                                             limit)
  elif isinstance(list_command, ListRegexCommand):
    return table_list_handler.list_regex_table(GOOGLE_BIGQUERY_CLIENT,
                                               dataset,
                                               list_command.table_name_pattern,
                                               limit)
  elif isinstance(list_command, ListWildcardCommand):
    return table_list_handler.list_wildcard_table(GOOGLE_BIGQUERY_CLIENT,
                                                  dataset,
                                                  list_command.table_prefix,
                                                  list_command.start_date,
                                                  list_command.end_date)
  else:
    raise ValueError("Unrecognised command type.")



def delete_table(del_command):
  dataset = del_command.dataset

  if isinstance(del_command, DeleteNamedCommand):
    table_delete_handler.delete_table_by_name(
        GOOGLE_BIGQUERY_CLIENT, dataset, del_command.table_name)
  elif isinstance(del_command, DeleteFileCommand):
    table_delete_handler.delete_table_using_file(
        GOOGLE_BIGQUERY_CLIENT, dataset, del_command.delete_file)
  elif isinstance(del_command, DeleteWildcardCommand):
    table_delete_handler.delete_table_with_wildcard(GOOGLE_BIGQUERY_CLIENT,
                                                    dataset,
                                                    del_command.table_prefix,
                                                    del_command.start_date,
                                                    del_command.end_date)
  elif isinstance(del_command, DeleteRegexCommand):
    table_delete_handler.delete_table_with_regex(GOOGLE_BIGQUERY_CLIENT,
                                                 dataset,
                                                 table_name_pattern)
  else:
    raise ValueError("Unrecognised command type.")

def __get_delete_tables(del_command):
  dataset = del_command.dataset

  if isinstance(del_command, DeleteNamedCommand):
    return [del_command.table_name]

  elif isinstance(del_command, DeleteFileCommand):
    arg_file_path = del_command.delete_file
    if os.path.isabs(arg_file_path):
      delete_file_path = arg_file_path
    else:
      working_path = os.getcwd()
      delete_file_path = os.path.join(working_path, arg_file_path)

    if not os.path.exists(delete_file_path):
      raise ValueError("Given file path doesn't exist: %s" % arg_file_path)

    with open(delete_file_path) as table_list_file:
      return table_list_file.readlines()

  elif isinstance(del_command, DeleteWildcardCommand):
    return table_list_handler.list_wildcard_table(GOOGLE_BIGQUERY_CLIENT,
                                                  dataset,
                                                  del_command.table_prefix,
                                                  del_command.start_date,
                                                  del_command.end_date)
  elif isinstance(del_command, DeleteRegexCommand):
    return table_list_handler.list_regex_table(GOOGLE_BIGQUERY_CLIENT,
                                               dataset,
                                               del_command.table_name_pattern,
                                               sys.maxint)
  else:
    raise ValueError("Unrecognised delete command.")

def __get_bigquery_service():
  credentials = GoogleCredentials.get_application_default()
  http = httplib2.Http()
  http = credentials.authorize(http)
  return build('bigquery', 'v2', credentials=credentials)

GOOGLE_BIGQUERY_CLIENT = __get_bigquery_service()
