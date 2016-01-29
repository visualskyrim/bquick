# -*- coding: utf-8 -*-

import sys
import os
import uuid
import copy
import time
from collections import namedtuple
from multiprocessing import Pool
from bquick.config_parser import get_config

BQ_CONFIG = get_config()
LIST_JOB_LOOK_BACK = 60 * 10
MAX_RUNNING_JOB = 20
JOB_LAUNCH_SIZE = 5
JOB_BLOCK_WAIT = 2


def thruhold_jobs(bq_client, max_jobs=MAX_RUNNING_JOB, block_wait=JOB_BLOCK_WAIT):
  undone_job_count = __get_undone_jobs_number(bq_client)
  while undone_job_count > max_jobs:
    print "Current running jobs are %d" % undone_job_count
    print "Will wait for %d seconds..." % block_wait
    time.sleep(block_wait)
    undone_job_count = __get_undone_jobs_number(bq_client)


def wait_all_job_finish(bq_client, job_id_list):

  for job_id in job_id_list:
    is_finished = __is_job_finished(bq_client, job_id)
    while not is_finished:
      time.sleep(2)
      is_finished = __is_job_finished(bq_client, job_id)

  return True


def get_table_packages(table_list, package_size=JOB_LAUNCH_SIZE):
  return list(__chunks(table_list, package_size))


def query_into(bq_client, table_name, dest_table_name, query_content, origin_dataset_name, dest_dataset_name):
  print "query data from [%s.%s] to [%s:%s]" % (origin_dataset_name, table_name, dest_dataset_name, table_name)

  project_id = BQ_CONFIG.project
  job_body = {
      'jobReference': {
          'projectId': project_id
      },
      "configuration": {
          "query": {
              "query": query_content,
              "destinationTable": {
                  "projectId": project_id,
                  "datasetId": dest_dataset_name,
                  "tableId": dest_table_name
              },
              "defaultDataset": {
                  "projectId": project_id,
                  "datasetId": origin_dataset_name
              },
              "createDisposition": "CREATE_IF_NEEDED",
              "writeDisposition": "WRITE_APPEND",
              "allowLargeResult": True
          }
      }
  }

  resp = bq_client.jobs().insert(
      projectId=project_id, body=job_body).execute()

  if 'jobReference' in resp and 'jobId' in resp['jobReference']:
    return resp['jobReference']['jobId']
  else:
    raise ValueError("Can't track job.")


def __get_job_status(bq_client, job_id):
  project_id = BQ_CONFIG.project
  job_status = bq_client.jobs().get(
      projectId=project_id, jobId=job_id).execute()

  return job_status


def __is_job_finished(bq_client, job_id):
  job_status = __get_job_status(bq_client, job_id)

  if 'status' not in job_status:
    raise ValueError("Can't find status in job response.")

  if 'state' not in job_status['status']:
    raise ValueError("Can't find status in job response.")

  if job_status['status']['state'] == "DONE":
    return True
  else:
    return False


def __chunks(l, n):
  """ Yield successive n-sized chunks from l.
  """
  for i in xrange(0, len(l), n):
    yield l[i:i+n]


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
  print ""
  return undone_job_count
