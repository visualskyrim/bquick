# -*- coding: utf-8 -*-

from bquick.command_parser import get_command
from bquick.bigquery import list_table, delete_table, copy_table


def main():
  """ Main function called to execute the command. """
  try:
    mode, command = get_command()

    if mode == 'ls':
      tables = list_table(command)
      for table_name in tables:
        print table_name
    elif mode == 'del':
      delete_table(command)
    elif mode == 'cp':
      copy_table(command)

  except Exception as e:
    raise e
