# -*- coding: utf-8 -*-

from commands import get_command

def main():

  """ Main function called to execute the command. """
  try:
    #print 'It works!'
    print get_command().integers

  except Exception as e:
    raise e
