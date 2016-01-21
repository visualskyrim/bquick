# -*- coding: utf-8 -*-

import sys
import os
import copy
import time
from collections import namedtuple
from multiprocessing import Pool
from bquick.config_parser import get_config
from bquick.bigquery import table_list_handler

BQ_CONFIG = get_config()

LIST_JOB_LOOK_BACK = 60 * 10
MAX_RUNNING_JOB = 20
COPY_JOB_LAUNCH_SIZE = 5
COPY_BLOCK_WAIT = 2


def copy_table_file(bq_client, dataset, dest, file_path):
  """Copy table with the name in a specific file.
  """
  if not os.path.exists(file_path):
    raise ValueError("Given file path doesn't exist: %s" % arg_file_path)

  with open(file_path) as table_list_file:
    table_list = [line.rstrip() for line in table_list_file.readlines()]
  __copy_table_list(bq_client, dataset, dest, table_list)


def copy_table_regex(bq_client, dataset, dest, table_name_pattern):
  """Copy the tables with the name that matches certern regex pattern.
  """
  table_list = table_list_handler.list_regex_table(
      bq_client,
      dataset,
      table_name_pattern,
      sys.maxint)
  __copy_table_list(bq_client, dataset, dest, table_list)


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
  __copy_table_list(bq_client, dataset, dest, table_list)


def __copy_table_list(
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
  package_list = __get_copy_launch_packages(table_name_list)

  for sub_table_name_list in package_list:
    # check running jobs
    undone_job_count = __get_undone_jobs_number(bq_client)
    while undone_job_count > MAX_RUNNING_JOB:
      print "Current running jobs are %d" % undone_job_count
      print "Will wait for %d seconds..." % COPY_BLOCK_WAIT
      time.sleep(COPY_BLOCK_WAIT)
      undone_job_count = __get_undone_jobs_number(bq_client)

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


def __get_undone_jobs_number(bq_client, look_back=LIST_JOB_LOOK_BACK):
  pageToken = None
  oldest_time = int(time.time())
  create_time_limit = oldest_time - LIST_JOB_LOOK_BACK
  undone_job_count = 0

  while True:
    try:
      resp = bq_client.jobs() \
          .list(projectId=BQ_CONFIG.project,
                stateFilter=["pending", "running"],
                pageToken=pageToken,
                allUsers=True,
                maxResults=50).execute()
    except Exception, e:
      print "Fail to fetch job information"
      raise e

    jobs = resp['jobs'] if 'jobs' in resp else []

    is_fetch_over = False

    for job_info in jobs:
      created_time_str = job_info['statistics']['creationTime']
      created_time_int = int(float(created_time_str) / 1000)
      if created_time_int < oldest_time:
        oldest_time = created_time_int

      if created_time_int < create_time_limit:
        is_fetch_over = True
        break
      undone_job_count += 1

    if is_fetch_over:
      break

    if 'nextPageToken' in resp:
      pageToken = resp['nextPageToken']
    else:
      break

  print "Running jobs: [%d]" % undone_job_count
  return undone_job_count


def __get_copy_launch_packages(table_list, package_size=COPY_JOB_LAUNCH_SIZE):
  return list(__chunks(table_list, package_size))


def __chunks(l, n):
  """ Yield successive n-sized chunks from l.
  """
  for i in xrange(0, len(l), n):
    yield l[i:i+n]
