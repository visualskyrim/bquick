# -*- coding: utf-8 -*-
import argparse
import sys
import os
import datetime
from collections import namedtuple

DEFAULT_START_DATE = "1970-01-01"
DEFAULT_END_DATE = datetime.datetime.strftime(datetime.datetime.now(),
                                              '%Y-%m-%d')

# list commands
ListCommand = namedtuple("ListCommand", "dataset limit")
ListWildcardCommand = namedtuple(
    "ListWildcardCommand", "dataset limit table_prefix start_date end_date")
ListRegexCommand = namedtuple(
    "ListRegexCommand", "dataset limit table_name_pattern")

# delete commands
DeleteNamedCommand = namedtuple("DeleteCommand", "dataset table_name")
DeleteFileCommand = namedtuple("DeleteFileCommand", "dataset delete_file")
DeleteWildcardCommand = namedtuple(
    "DeleteWildcardCommand", "dataset table_prefix start_date end_date")
DeleteRegexCommand = namedtuple(
    "DeleteRegexCommand", "dataset table_name_pattern")

# copy commands
CopyFileCommand = namedtuple("CopyFileCommand", "dataset dest copy_file")
CopyWildcardCommand = namedtuple(
    "CopyWildcardCommand", "dataset dest table_prefix start_date end_date")
CopyRegexCommand = namedtuple(
    "CopyRegexCommand", "dataset dest table_name_pattern")

# data delete commands
DataDeleteNamedCommand = namedtuple(
    "DataDeleteCommand", "dataset condition table_name")
DataDeleteFileCommand = namedtuple(
    "DataDeleteFileCommand", "dataset condition delete_file")
DataDeleteWildcardCommand = namedtuple(
    "DataDeleteWildcardCommand",
    "dataset condition table_prefix start_date end_date")
DataDeleteRegexCommand = namedtuple(
    "DataDeleteRegexCommand", "dataset condition table_name_pattern")


def get_command():
  return parse_args(sys.argv[1:])


def parse_args(argv):
  """This function will parse the args command line args.
  The function will return one of:
    - ListRegexCommand
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
  ls_ex_group = __add_wildcard_arg(ls_ex_group)
  ls_ex_group = __add_regex_arg(ls_ex_group)
  ls_parser.add_argument("-l",
                         "--limit",
                         type=int,
                         help="Limit to number of tables to show.",
                         default=50)

  # parser for deleting tables
  del_parser = sub_parsers.add_parser(
      "del", description="Mode: delete tables.")
  del_ex_group = del_parser.add_mutually_exclusive_group()
  del_ex_group = __add_wildcard_arg(del_ex_group)
  del_ex_group = __add_regex_arg(del_ex_group)
  del_ex_group = __add_file_arg(del_ex_group)
  del_ex_group = __add_name_arg(del_ex_group)

  # parser for copying tables
  cp_parser = sub_parsers.add_parser("cp", description="Mode: copy tables.")
  cp_parser.add_argument("-d",
                         "--destination",
                         help="The destination dataset.",
                         default=None)
  cp_ex_group = cp_parser.add_mutually_exclusive_group()
  cp_ex_group = __add_regex_arg(cp_ex_group)
  cp_ex_group = __add_wildcard_arg(cp_ex_group)
  cp_ex_group = __add_file_arg(cp_ex_group)

  # parser for delete data from tables
  ddel_parser = sub_parsers.add_parser(
      "ddel", description="Mode: delete data.")
  ddel_parser.add_argument("-c",
                           "--condition",
                           help="The condition to delete row.",
                           default=None)
  ddel_ex_group = ddel_parser.add_mutually_exclusive_group()
  ddel_ex_group = __add_wildcard_arg(ddel_ex_group)
  ddel_ex_group = __add_regex_arg(ddel_ex_group)
  ddel_ex_group = __add_file_arg(ddel_ex_group)
  ddel_ex_group = __add_name_arg(ddel_ex_group)

  args = top_parser.parse_args(argv)
  return args.mode, __parse_command(args)


def __add_wildcard_arg(arg_collection):
  arg_collection.add_argument(
      "-w", "--wildcard", nargs='+',
      help="""Wildcard args:\n
              1. Table name without wildcard date suffix.\n
              2. Table start date string in YYYY-mm-dd.\n
              3. Table end date in YYYY-mm-dd.""")
  return arg_collection


def __add_regex_arg(arg_collection):
  arg_collection.add_argument(
      "-r", "--regex",
      help="Regex Expression to match the table name.")
  return arg_collection


def __add_file_arg(arg_collection):
  arg_collection.add_argument(
      "-f",
      "--file",
      help="""The file which contains the list of names \
            of tables to deletes. \
            Each line represents a table.""")
  return arg_collection


def __add_name_arg(arg_collection):
  arg_collection.add_argument("-n",
                              "--name",
                              help="""The name of the table to be matched.""")
  return arg_collection


def __parse_command(args):
  # parse args into command
  dataset = args.dataset
  if args.mode == "ls":
    # parse the command for listing tables
    limit = args.limit

    if args.regex is not None:
      return ListRegexCommand(dataset=dataset,
                              limit=limit,
                              table_name_pattern=args.regex)
    elif args.wildcard is not None:
      table_prefix, start_date, end_date = \
          __extract_wildcard_command_parts(args.wildcard)

      return ListWildcardCommand(dataset=dataset,
                                 limit=limit,
                                 table_prefix=table_prefix,
                                 start_date=start_date,
                                 end_date=end_date)
    else:
      return ListCommand(dataset=dataset, limit=limit)

  elif args.mode == "del":
    # parse the command for deleting tables
    if args.regex is not None:
      return DeleteRegexCommand(dataset=dataset,
                                table_name_pattern=args.regex)
    elif args.wildcard is not None:
      table_prefix, start_date, end_date = \
          __extract_wildcard_command_parts(args.wildcard)

      return DeleteWildcardCommand(dataset=dataset,
                                   table_prefix=table_prefix,
                                   start_date=start_date,
                                   end_date=end_date)
    elif args.file is not None:
      return DeleteFileCommand(dataset=dataset,
                               delete_file=__get_abs_file_path(args.file))
    elif args.name is not None:
      return DeleteNamedCommand(dataset=dataset, table_name=args.name)
    else:
      raise ValueError("Unrecognised delete mode.")

  elif args.mode == "cp":
    # parse the command for copying tables
    if args.destination is None:
      raise ValueError("No destination dateset.")

    if args.regex is not None:
      return CopyRegexCommand(dataset=dataset,
                              dest=args.destination,
                              table_name_pattern=args.regex)
    elif args.wildcard is not None:
      table_prefix, start_date, end_date = \
          __extract_wildcard_command_parts(args.wildcard)
      return CopyWildcardCommand(dataset=dataset,
                                 dest=args.destination,
                                 table_prefix=table_prefix,
                                 start_date=start_date,
                                 end_date=end_date)
    elif args.file is not None:
      return CopyFileCommand(dataset=dataset,
                             dest=args.destination,
                             copy_file=__get_abs_file_path(args.file))
    else:
      raise ValueError("Unrecognised copy mode.")
  elif args.mode == "ddel":
    if args.condition is None:

  else:
    raise ValueError("Unrecognised mode: %s" % args.mode)


def __extract_wildcard_command_parts(wildcard_args):
  if len(wildcard_args) > 3 or len(wildcard_args) == 2:
    raise "The length of wildcard args should be 1 or 3."

  table_prefix = wildcard_args[0]

  if len(wildcard_args) == 1:
    start_date = DEFAULT_START_DATE
    end_date = DEFAULT_END_DATE
  else:
    start_date = wildcard_args[1]
    end_date = wildcard_args[2]

  # check the date string format
  try:
    datetime.datetime.strptime(start_date, "%Y-%m-%d")
    datetime.datetime.strptime(end_date, "%Y-%m-%d")
  except Exception as e:
    raise ValueError("Wrong format of date arguments. \
                     Please use YYYY-mm-dd instead.")

  return table_prefix, start_date, end_date


def __get_abs_file_path(file_path):
  arg_file_path = file_path
  if os.path.isabs(arg_file_path):
    abs_path = arg_file_path
  else:
    working_path = os.getcwd()
    abs_path = os.path.join(working_path, arg_file_path)

  if not os.path.exists(abs_path):
    raise ValueError("Given file path doesn't exist: %s" % file_path)

  return abs_path
