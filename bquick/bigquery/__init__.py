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

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

def list_table(list_command):
  dataset = list_command.dataset
  limit = list_command.limit

  if isinstance(list_command) is ListCommand:
    table_list_handler.__list_all_table(GOOGLE_BIGQUERY_CLIENT, dataset, limit)
  elif isinstance(list_command) is ListRegexCommand:
    table_list_handler.list_regex_table(GOOGLE_BIGQUERY_CLIENT,
                                        dataset,
                                        list_command.regex,
                                        limit)
  elif isinstance(list_command) is ListWildcardCommand:
    table_list_handler.list_wildcard_table(GOOGLE_BIGQUERY_CLIENT,
                                           dataset,
                                           list_command.table_prefix)




def __get_bigquery_service():
  credentials = GoogleCredentials.get_application_default()
  self.http = httplib2.Http()
  self.http = credentials.authorize(self.http)
  return build('bigquery', 'v2', credentials=credentials)

GOOGLE_BIGQUERY_CLIENT = __get_bigquery_service()
