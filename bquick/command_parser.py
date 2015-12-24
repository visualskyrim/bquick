# -*- coding: utf-8 -*-
import argparse
import sys
import datetime
from collections import namedtuple

DEFAULT_START_DATE = "1970-01-01"
DEFAULT_END_DATE = datetime.datetime.strftime(datetime.datetime.now(),
                                              '%Y-%m-%d')

ListCommand = namedtuple("ListCommand", "dataset limit")
ListWildcardCommand = namedtuple("ListWildcardCommand",
                                 "dataset limit table_prefix start_date end_date")
ListRegexCommand = namedtuple("ListRegexCommand", "dataset limit table_name_pattern")



def get_command():
  return parse_args(sys.argv[1:])

def parse_args(argv):
  """This function will parse the args command line args.
  The function will return one of:
    - RegexCommand
    - ListWildcardCommand
    - ListCommand
  """
  top_parser = argparse.ArgumentParser(
      description="Google BigQuery management tool.")
  top_parser.add_argument("dataset", help="""The dateset.""")
  sub_parsers = top_parser.add_subparsers(dest="mode", help="Command modes.")

  # parser for listing tables
  ls_parser = sub_parsers.add_parser("ls", description="Mode: list tables.")
  ls_ex_group = ls_parser.add_mutually_exclusive_group()

  ls_ex_group.add_argument(
      "-w", "--wildcard", nargs='+',
      help="""Wildcard args:\n
              1. Table name without wildcard date suffix.\n
              2. Table start date string in YYYY-mm-dd.\n
              3. Table end date in YYYY-mm-dd.""")

  ls_ex_group.add_argument(
      "-r", "--regex",
      help="Regex Expression to match the table name.")

  ls_parser.add_argument("-l",
                         "--limit",
                         type=int,
                         help="Limit to number of tables to show.",
                         default=50)

  # parser for deleting tables
  del_parser = sub_parsers.add_parser("del", description="Mode: delete tables.")
  del_parser.add_argument("-f",
                          "-file",
                          help="""The file which contains the name of tables \
                                  to deletes. Each line represents a table.""")

  args = top_parser.parse_args(argv)
  return args.mode, __parse_command(args)


def __parse_command(args):
  # parse args into command
  dataset = args.dataset
  if args.mode == "ls":
    limit = args.limit

    if args.regex is not None:
      return RegexCommand(dataset=dataset, limit=limit, table_name_pattern=args.regex)
    elif args.wildcard is not None:
      wildcard_args = args.wildcard

      if len(wildcard_args) > 3 or len(wildcard_args) == 2:
        raise "The length of wildcard args should be 1 or 3."

      table_prefix = wildcard_args[0]

      if len(wildcard_args) == 1:
        start_date = DEFAULT_START_DATE
        end_date   = DEFAULT_END_DATE
      else:
        start_date = wildcard_args[1]
        end_date   = wildcard_args[2]

      # check the date string format
      try:
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
      except Exception as e:
        print str(e)
        raise ValueError("Wrong format of date arguments. \
                         Please use YYYY-mm-dd instead.")

      return ListWildcardCommand(dataset=dataset,
                                 limit=limit,
                                 table_prefix=table_prefix,
                                 start_date=start_date,
                                 end_date=end_date)
    else:
      return ListCommand(dataset=dataset, limit=limit)
  elif args.mode == "del":
    pass
  else:
    raise ValueError("Unrecognised mode: %s" % args.mode)
