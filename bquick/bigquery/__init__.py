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
    table_list_handler, table_delete_handler, table_copy_handler
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
    CopyRegexCommand


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
    copy_file_path = del_command.delete_file
    if not os.path.exists(arg_file_path):
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


def __get_copy_tables(cp_command):
  dataset = cp_command.dataset

  if isinstance(cp_command, CopyFileCommand):
    arg_file_path = del_command.delete_file
    if not os.path.exists(arg_file_path):
      raise ValueError("Given file path doesn't exist: %s" % arg_file_path)

    with open(arg_file_path) as table_list_file:
      return table_list_file.readlines()
  elif isinstance(cp_command, CopyWildcardCommand):
    return table_list_handler.list_wildcard_table(GOOGLE_BIGQUERY_CLIENT,
                                                  dataset,
                                                  del_command.table_prefix,
                                                  del_command.start_date,
                                                  del_command.end_date)
  elif isinstance(cp_command, CopyRegexCommand):
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
