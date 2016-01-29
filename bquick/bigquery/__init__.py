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

from bquick.bigquery import \
    table_list_handler, table_delete_handler, table_copy_handler, data_delete_handler
from bquick.command_parser import \
    ListCommand, \
    ListRegexCommand, \
    ListWildcardCommand, \
    DeleteFileCommand, \
    DeleteNamedCommand, \
    DeleteRegexCommand, \
    DeleteWildcardCommand, \
    CopyFileCommand, \
    CopyWildcardCommand, \
    CopyRegexCommand, \
    DataDeleteFileCommand, \
    DataDeleteNamedCommand, \
    DataDeleteRegexCommand, \
    DataDeleteWildcardCommand


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
                                                  list_command.end_date,
                                                  limit)
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
                                                 del_command.table_name_pattern)
  else:
    raise ValueError("Unrecognised command type.")


def copy_table(cp_command):
  dataset = cp_command.dataset
  dest = cp_command.dest

  if isinstance(cp_command, CopyFileCommand):
    copy_file_path = cp_command.copy_file
    if not os.path.exists(copy_file_path):
      raise ValueError("Given file path doesn't exist: %s" % copy_file_path)

    table_copy_handler.copy_table_file(
        GOOGLE_BIGQUERY_CLIENT, dataset, dest, copy_file_path)

  elif isinstance(cp_command, CopyWildcardCommand):
    table_copy_handler.copy_table_wildcard(
        GOOGLE_BIGQUERY_CLIENT,
        dataset,
        dest,
        cp_command.table_prefix,
        cp_command.start_date,
        cp_command.end_date)

  elif isinstance(cp_command, CopyRegexCommand):
    table_copy_handler.copy_table_regex(
        GOOGLE_BIGQUERY_CLIENT, dataset, dest, cp_command.table_name_pattern)

  else:
    raise ValueError("Unrecognised delete command.")


def delete_data(ddel_command):
  dataset = ddel_command.dataset
  condition = ddel_command.condition

  if isinstance(ddel_command, DataDeleteNamedCommand):
    data_delete_handler.delete_data_by_name(
        GOOGLE_BIGQUERY_CLIENT, dataset, condition, ddel_command.table_name)
  elif isinstance(ddel_command, DataDeleteFileCommand):
    data_delete_handler.delete_data_using_file(
        GOOGLE_BIGQUERY_CLIENT, dataset, condition, ddel_command.delete_file)
  elif isinstance(ddel_command, DataDeleteWildcardCommand):
    data_delete_handler.delete_data_with_wildcard(GOOGLE_BIGQUERY_CLIENT,
                                                  dataset,
                                                  condition,
                                                  ddel_command.table_prefix,
                                                  ddel_command.start_date,
                                                  ddel_command.end_date)
  elif isinstance(ddel_command, DataDeleteRegexCommand):
    data_delete_handler.delete_data_with_regex(GOOGLE_BIGQUERY_CLIENT,
                                               dataset,
                                               condition,
                                               ddel_command.table_name_pattern)
  else:
    raise ValueError("Unrecognised command type.")


def __get_bigquery_service():
  credentials = GoogleCredentials.get_application_default()
  http = httplib2.Http()
  http = credentials.authorize(http)
  return build('bigquery', 'v2', credentials=credentials)

GOOGLE_BIGQUERY_CLIENT = __get_bigquery_service()
