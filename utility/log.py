"""
Utility: Log

Methods of logging actions
"""


import sys
import time
import logging
import traceback
import os


# Level strings
LEVEL_STRINGS = {
  logging.INFO:'INFO',
  logging.DEBUG:'DEBUG',
  logging.WARN:'WARN',
  logging.ERROR:'ERROR',
  logging.CRITICAL:'CRITICAL',
}

# Default level string
DEFAULT_LEVEL = logging.INFO

# Format of the logging
LOG_FORMAT = '%(level)s:%(time)s:%(text)s'

# Store run options here to disable logging, unless we're in Verbose mode
RUN_OPTIONS = None


def Log(text, level=DEFAULT_LEVEL, print_stack=0, options=None):
  """Log information about progress.
  
  Args:
    options: unused, inserted for compatibility with RunMan log() calls
  """
  # If we are not in verbose mode, dont print out logging info
  global RUN_OPTIONS
  if RUN_OPTIONS and not RUN_OPTIONS.get('verbose', False):
    return
  
  time_object = time.localtime(time.time())
  
  # Build the time string
  time_str = '%s/%02d/%02d %02d:%02d:%02d' % (time_object.tm_year, time_object.tm_mon, 
      time_object.tm_mday, time_object.tm_hour, time_object.tm_min, 
      time_object.tm_sec)
  
  # Get the level string, or default (unknown)
  level_string = LEVEL_STRINGS.get(level, 'UNKNOWN')
  
  # Create format data
  format_data = {'level':level_string, 'text':text, 'time':time_str}
  
  # Format things
  #TODO(g): Do error detection for this
  output = LOG_FORMAT % format_data
  
  # If we want to add the stack, pull off N items from the stack
  if print_stack > 0:
    # Dont include this function, and get the requested number of layers
    stack_items = traceback.extract_stack()[-(print_stack+1):-1]
    stack_output = ''
    for item in stack_items:
      if stack_output:
        stack_output += ' -> '
      
      stack_output += '%s.%s[%s]' % (os.path.basename(item[0]), item[2], item[1])
  
    output = '%s [[%s]]' % (output, stack_output)
  
  # Append newline
  output += '\n'
  
  #TODO(g): Set up logging handler with output file, rotations, and other 
  #   things like that.
  #TODO(g): Make a log each run?  Or append to a log?  TBD.
  sys.stderr.write(output)


# Compatibility with RunMan's different capitalization
log = Log


