# -*- coding: utf-8 -*-
import argparse


def get_command():
  parser = argparse.ArgumentParser(description='Google BigQuery management tool.')
  parser.add_argument("mode")

  args = parser.parse_args("-l", \
                           "--limit", \
                           help="limit to number of tables to show")

  return args.mode, args.limit
