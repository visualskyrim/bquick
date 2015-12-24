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
    command = parse_args(test_argv)
    self.assertTrue(isinstance(command, ListCommand))
    self.assertEqual(command.dataset, 'test_dataset')

  def test_parse_ls_wildcard(self):
    """Usecase of wilcard
    """
    test_argv = ['test_dataset', 'ls', '-w', 'test_table',
                 '2015-12-12', '2015-12-13']
    command = parse_args(test_argv)
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
    command = parse_args(test_argv)
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
