# -*- coding: utf-8 -*-


from bquick.command_parser import get_command

def main():

  """ Main function called to execute the command. """
  try:
    #print 'It works!'
    args = get_command()
    print args

  except Exception as e:
    raise e
