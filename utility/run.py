"""
Utility: Run

Methods for running shell commands
"""

import commands

from error import Error
from log import Log


# Commands to be run on commit
RUN_COMMIT_LIST = []


def ClearRunCommitList():
  """Reset this module by clearing the RUN_COMMIT_LIST, which we should we before starting each new dynamic deployment."""
  global RUN_COMMIT_LIST
  
  RUN_COMMIT_LIST = []


def Run(cmd):
  """Returns: (status, output) - (int, str)"""
  (status, output) = commands.getstatusoutput(cmd)
  
  return (status, output)


def RunOnCommit(cmd, message_on_error, options, status_ok=None):
  """Run this command on commit.  No return, as all work is deferred.
  
  args:
    cmd: string, command to run
    
    message_on_error: string or None.  If an error occurs, then this is the 
        error message to print.  If None instead of string, then errors are
        ignored.  All status codes are assumes to be OK.
      
    status_ok: list of ints (default None), if None the list is initiatialized to [0], 
        otherwise a list of acceptable integers can be passed in
  
  Returns: None
  
  TODO(g): This needs to queue up work to be performed later, after associating it with a 
      file/service/action/etc to be performed, so that each thing only gets updated once.  
      Currently, files could be copied when not needed to be, and could have permissions
      changed multiple times.
  """
  global RUN_COMMIT_LIST
  
  Log('Run on commit: %s' % cmd)
  
  # Set default status OK list, if unset or it is not a list
  if status_ok == None or type(status_ok) not in (list, tuple):
    status_ok = [0]
  
  # Create the command data, so that we can run this command
  cmd_data = {'cmd':cmd, 'message_on_error':message_on_error, 'status_ok':status_ok}
  
  # Add this to our run commit list
  RUN_COMMIT_LIST.append(cmd_data)


def Commit_Commands(options):
  """Commit all our stored commands"""
  global RUN_COMMIT_LIST
  
  for cmd_data in RUN_COMMIT_LIST:
    # Print the command we are going to run
    print cmd_data['cmd']
    
    # Run the command
    (status, output) = Run(cmd_data['cmd'])
  
    # If this was not successful, fail and print the error message
    if status not in cmd_data['status_ok']:
      if cmd_data['message_on_error'] != None:
        Error('%s: %s' % (cmd_data['message_on_error'], output), options)
      else:
        Error('COMMAND FAILED: Status: %s: %s: %s' % (status, cmd_data['cmd'], output), options)

