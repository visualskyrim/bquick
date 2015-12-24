# -*- coding: utf-8 -*-

import sys
import os.path
import ConfigParser
import json
from collections import namedtuple

# config named tuple
ConfigTuple = namedtuple("ConfigTuple", "account project")

USER_ROOT = os.path.expanduser("~")
DEFAULT_CONFIG_FOLDER = os.path.join(USER_ROOT, ".config", "gcloud")
# BQUICK_CONFIG_ROOT = os.path.join(USER_ROOT, ".config", "bquick")

if not os.path.exists(DEFAULT_CONFIG_FOLDER):
  raise IOError("Can't find gcloud settings folder in %s." \
                % DEFAULT_CONFIG_FOLDER)

DEFAULT_CONFIG_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_FOLDER, "properties")

if not os.path.exists(DEFAULT_CONFIG_CONFIG_FILE):
  DEFAULT_CONFIG_CONFIG_FILE_2 = os.path.join(
      DEFAULT_CONFIG_FOLDER, "configurations", "config_default")
  if os.path.exists(DEFAULT_CONFIG_CONFIG_FILE_2):
    DEFAULT_CONFIG_CONFIG_FILE = DEFAULT_CONFIG_CONFIG_FILE_2
  else:
    raise IOError("Can't find gcloud settings file in %s and %s." \
                % (DEFAULT_CONFIG_CONFIG_FILE, DEFAULT_CONFIG_CONFIG_FILE_2))

CORE_CONFIG_SECTION = "core"
ACCOUNT_CONFIG_KEY = "account"
PROJECT_CONFIG_KEY = "project"

def get_config(config_file=""):
  if config_file == "":
    return __get_gcloud_config(DEFAULT_CONFIG_CONFIG_FILE)
  else:
    return __get_gcloud_config(config_file)


def __get_gcloud_config(config_file):
  """Get  default config from given file path.

  The function will return the config object consisting of account and project
  information.
  If the config file is not in the right format, the function will throw an
  exception.

  Args:
    - config_file: the file path to the config file.
  """
  try:
    config_path = os.path.expanduser(config_file)

    # Read the configuration file
    config_file = ConfigParser.ConfigParser()
    config_file.read(config_path)

    google_account = config_file.get(CORE_CONFIG_SECTION, ACCOUNT_CONFIG_KEY)
    google_project = config_file.get(CORE_CONFIG_SECTION, PROJECT_CONFIG_KEY)
  except Exception as e:
    raise

  return ConfigTuple(account=google_account, project=google_project)
