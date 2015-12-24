# -*- coding: utf-8 -*-

import httplib2
import urllib
import uuid
import time
import json
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials
from urllib2 import HTTPError

from bquick.bigquery import table_list_handler
from bquick.command_parser import ListCommand, \
                                  ListRegexCommand, \
                                  ListWildcardCommand

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

def list_table(list_command):
  dataset = list_command.dataset
  limit = list_command.limit

  if isinstance(list_command, ListCommand):
    return table_list_handler.list_all_table(GOOGLE_BIGQUERY_CLIENT, dataset, limit)
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




def __get_bigquery_service():
  credentials = GoogleCredentials.get_application_default()
  http = httplib2.Http()
  http = credentials.authorize(http)
  return build('bigquery', 'v2', credentials=credentials)

GOOGLE_BIGQUERY_CLIENT = __get_bigquery_service()
