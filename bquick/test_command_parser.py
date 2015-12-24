# -*- coding: utf-8 -*-

import unittest
import datetime
from bquick.command_parser import parse_args
from bquick.command_parser import ListCommand, \
                                 ListWildcardCommand, \
                                 ListRegexCommand

class CommandParserTest(unittest.TestCase):

  """Test for command_parser.py"""

  def test_parse_ls(self):
    """Basic usecase of ls
    """
    test_argv = ['test_dataset', 'ls']
    mode, command = parse_args(test_argv)
    self.assertEqual(mode, "ls")
    self.assertTrue(isinstance(command, ListCommand))
    self.assertEqual(command.dataset, 'test_dataset')

  def test_parse_ls_wildcard(self):
    """Usecase of wilcard
    """
    test_argv = ['test_dataset', 'ls', '-w', 'test_table',
                 '2015-12-12', '2015-12-13']
    mode, command = parse_args(test_argv)
    self.assertEqual(mode, "ls")
    self.assertTrue(isinstance(command, ListWildcardCommand))
    self.assertEqual(command.dataset, 'test_dataset')
    self.assertEqual(command.start_date, '2015-12-12')
    self.assertEqual(command.end_date, '2015-12-13')

  def test_parse_list_wildcard_without_date(self):
    """Usecase of wilcard:
    without start date and end date
    """
    expect_default_end_date = datetime.datetime.strftime(
        datetime.datetime.now(), '%Y-%m-%d')

    test_argv = ['test_dataset', 'ls', '-w', 'test_table']
    mode, command = parse_args(test_argv)
    self.assertEqual(mode, "ls")
    self.assertTrue(isinstance(command, ListWildcardCommand))
    self.assertEqual(command.dataset, 'test_dataset')
    self.assertEqual(command.start_date, '1970-01-01')
    self.assertEqual(command.end_date, expect_default_end_date)

  def test_parse_list_wildcard_with_wrong_date_args(self):
    """Usecase of wildcard:
    without only start date nor end date
    """
    test_argv = ['test_dataset', 'ls', '-w', 'test_table',
                 '20151218', '2015-12-13']
    self.assertRaises(ValueError, lambda: parse_args(test_argv))

  def test_parse_del_name(self):
    """Usecase of using argument to id the table to be deleted.
    """
    test_argv = ['test_dataset', 'del', '-n', 'test_table']
    mode, command = parse_args(test_argv)
    self.assertEqual(mode, "del")
    self.assertEqual(command.dataset, "test_dataset")
    self.assertEqual(command.table_name, "test_table")

  def test_parse_del_file(self):
    """Usecase of using file to list the tables to be deleted.
    """
    test_argv = ['test_dataset', 'del', '-f', '/path/to/hold/the/file.txt']
    mode, command = parse_args(test_argv)
    self.assertEqual(mode, 'del')
    self.assertEqual(command.dataset, 'test_dataset')
    self.assertEqual(command.delete_file, '/path/to/hold/the/file.txt')

  def test_parse_del_regex(self):
    """Usecase of using the regex to match tables to be deleted.
    """
    test_argv = ['test_dataset', 'del', '-r', 'super_awesome_regex']
    mode, command = parse_args(test_argv)
    self.assertEqual(mode, 'del')
    self.assertEqual(command.dataset, 'test_dataset')
    self.assertEqual(command.table_name_pattern, 'super_awesome_regex')

  def test_parse_del_wildcard(self):
    """Usecase of using the wildcard to match the tables to be delete.
    """
    test_argv = ['test_dataset', 'del', '-w', 'test_table']
    mode, command = parse_args(test_argv)
    self.assertEqual(mode, 'del')
    self.assertEqual(command.dataset, 'test_dataset')
    self.assertEqual(command.table_prefix, 'test_table')
