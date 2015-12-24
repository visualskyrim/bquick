# -*- coding: utf-8 -*-

import datetime

from bquick.config_parser import get_config

TIME_OUT_SEC = 100
QUERY_MAX_RESULT = 5000


def list_all_table(bq_client, dataset, limit):
  LIST_TABLE_QUERY = "SELECT table_id \
                      FROM %s.__TABLES__ \
                      LIMIT %d" % (dataset, limit)
  query_result = __query(bq_client, dataset, LIST_TABLE_QUERY)
  table_list = [row['f'][0]['v'] for row in query_result]
  return table_list

def list_regex_table(bq_client, dataset, regex, limit):
  LIST_TABLE_REGEX_QUERY = "SELECT table_id \
                            FROM %s.__TABLES__ \
                            WHERE (REGEXP_MATCH(table_id, r\'%s\')) \
                            LIMIT %d" % \
                                (dataset, regex, limit)
  query_result = __query(bq_client, dataset, LIST_TABLE_REGEX_QUERY)
  table_list = [row['f'][0]['v'] for row in query_result]
  return table_list

def list_wildcard_table(bq_client,
                        dataset,
                        wildcard_prefix,
                        start_date, end_date):
  wildcard_prefix_len = len(wildcard_prefix)
  start_date_str = start_date.replace('-', '')
  end_date_str = end_date.replace('-', '')

  start_table_id = wildcard_prefix + start_date_str
  end_table_id = wildcard_prefix + end_date_str

  LIST_TABLE_WILDCARD_QUERY = "SELECT table_id \
                               FROM %s.__TABLES__ \
                               WHERE SUBSTR(table_id, 0, %d) == \'%s\' \
                               AND table_id >= \'%s\' \
                               AND table_id <= \'%s\'" % (
                                   dataset, \
                                   wildcard_prefix_len, \
                                   wildcard_prefix, \
                                   start_table_id, end_table_id)

  query_result = __query(bq_client, dataset, LIST_TABLE_WILDCARD_QUERY)
  table_list = [row['f'][0]['v'] for row in query_result]

  filter_table_list = [table_id \
                       for table_id in table_list \
                       if __is_date_suffix(table_id[-8:])]
  return filter_table_list


def __is_date_suffix(suffix):
  try:
    datetime.datetime.strptime(suffix, '%Y%m%d')
    return True
  except Exception as e:
    return False

def __query(bq_client, dataset, query_text):
  query_body = {
    'timeoutMs': TIME_OUT_SEC * 1000,
    'maxResults': QUERY_MAX_RESULT,
    'defaultDataset': {
        'projectId': BQ_CONFIG.project,
        'datasetId': dataset
    },
    "query": query_text
  }

  # fire query
  resp = bq_client.jobs() \
      .query(projectId=BQ_CONFIG.project, body=query_body) \
      .execute()

  query_id = resp['jobReference']['jobId']
  result_set = []

  # get query result
  result_resp = bq_client.jobs().getQueryResults(
      projectId=BQ_CONFIG.project,
      jobId=query_id,
      timeoutMs=TIME_OUT_SEC * 1000).execute()

  if 'errors' in result_resp:
    raise IOError("Error occurred when paging the result.")

  if 'rows' not in result_resp:
    return []

  result_set.extend(result_resp['rows'])

  next_token = ''
  if 'pageToken' in result_resp:
    next_token = result_resp['pageToken']

  # keep getting query result
  while next_token is not None and next_token != '':
    result_resp = bq_client.jobs().getQueryResults(
        projectId=BQ_CONFIG.project,
        jobId=query_id,
        timeoutMs=timeout * 1000,
        pageToken=next_token).execute()

    if 'errors' in result_resp:
      raise ValueError("Error occurred when paging the result...")

    if 'rows' not in result_resp:
      result_set.extend([])
    else:
      result_set.extend(result_resp['rows'])

    next_token = ''
    if 'pageToken' in result_resp:
      next_token = result_resp['pageToken']

  return result_set

BQ_CONFIG = get_config()
