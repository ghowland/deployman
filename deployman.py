#!/usr/bin/python
"""
DeployMan - Management of things that need to be dynamically deployed.  Used after initial deployment of base configuration, which is more static, less dynamic.

DeployMan is a combination of RunMan and Sysync, which connects to a remote server to get work that needs to be deployed for this machine's component/service instances.

Once the dynamic deployment information is gathered, then a process like Sysync will apply the required changes to the target node running DeployMan.

Copyright Geoff Howland, 2014.  MIT License.

TODO:
  - ...
  -
"""


import sys
import os
import getopt
import yaml
import json
import pprint
import logging


import utility
from utility.log import log


# Default configuration file path
DEFAULT_CONFIG_PATH = 'deployman.yaml'

# Default handler path
DEFAULT_HANDLER_PATH = 'handlers/defaults/'

# Output formats we support
OUTPUT_FORMATS = ['json', 'yaml', 'pprint']


def ProcessCommand(config, command_options, command_args):
  """Process a command against this run_spec_path"""
  output_data = {}
  
  
  while utility.client.RUNNING:
    #try:
    if 1:
      # Become a client forever, requesting dynamic deployments
      utility.client.ProcessRequestsForever(config, command_options, command_args)
    
    #except Exception, e:
    #  log('ProcessCommand Client ProcessRequestsForever Exception: %s' % e, level=logging.ERROR)
  
  return output_data


def FormatAndOuput(result, command_options):
  """Format the output and return it"""
  # PPrint
  if command_options['format'] == 'pprint':
    pprint.pprint(result)
  
  # YAML
  elif command_options['format'] == 'yaml':
    print yaml.dump(result)
  
  # JSON
  elif command_options['format'] == 'json':
    print json.dumps(result)
  
  else:
    raise Exception('Unknown output format "%s", result as text: %s' % (command_options['format'], result))


def Usage(error=None):
  """Print usage information, any errors, and exit.

  If errors, exit code = 1, otherwise 0.
  """
  if error:
    print '\nerror: %s' % error
    exit_code = 1
  else:
    exit_code = 0
  
  print
  print 'usage: %s [options]' % os.path.basename(sys.argv[0])
  print
  print 'example usage: "python %s --cconfig deployman.yaml"' % os.path.basename(sys.argv[0])
  print
  print
  print 'Options:'
  print
  print '  -h, -?, --help              This usage information'
  print '  -v, --verbose               Verbose output'
  print '  -f, --format <format>       Format output, types: %s' % ', '.join(OUTPUT_FORMATS)
  print '  -c, --config <path>         Path to config file (Format specified by suffic: (.yaml, .json)'
  print '  --override-host <hostname>  Hostname to run jobs as.  Allows '
  print
  
  sys.exit(exit_code)


def Main(args=None):
  if not args:
    args = []

  long_options = ['help', 'format=', 'verbose', 'config=']
  
  try:
    (options, args) = getopt.getopt(args, '?hvnsc:f:', long_options)
  except getopt.GetoptError, e:
    Usage(e)
  
  # Dictionary of command options, with defaults
  command_options = {}
  command_options['remote'] = False   # Remote invocation.  When quitting or Error(), report back remotely with details.
  command_options['platform'] = utility.platform.GetPlatform()
  #command_options['verbose'] = False
  command_options['verbose'] = True
  command_options['format'] = 'pprint'
  command_options['config_path'] = DEFAULT_CONFIG_PATH
  command_options['handler_data_path'] = DEFAULT_HANDLER_PATH
  command_options['files_path'] = None
  command_options['override_host'] = None
  
  
  # Process out CLI options
  for (option, value) in options:
    # Help
    if option in ('-h', '-?', '--help'):
      Usage()
    
    # Verbose output information
    elif option in ('-v', '--verbose'):
      command_options['verbose'] = True
    
    # Noninteractive.  Doesnt use STDIN to gather any missing data.
    elif option in ('-c', '--config'):
      command_options['config_path'] = value
    
    # Format output
    elif option in ('-f', '--format'):
      if value not in (OUTPUT_FORMATS):
        Usage('Unsupported output format "%s", supported formats: %s' % (value, ', '.join(OUTPUT_FORMATS)))
      
      command_options['format'] = value
    
    # Overrride: Host name for running jobs
    elif option == '--override-host':
      command_options['override_host'] = value
    
    # Invalid option
    else:
      Usage('Unknown option: %s' % option)
  
  
  # Store the command options for our logging
  utility.log.RUN_OPTIONS = command_options
  
  
  # Load the configuration
  if not os.path.isfile(command_options['config_path']):
    Usage('Config file does not exist: %s' % command_options['config_path'])
  
  try:
    config = yaml.load(open(command_options['config_path']))
    
    # Put the files in the default temp path
    command_options['files_path'] = config['deploy_temp_path']
  
  except Exception, e:
    Usage('Failed to load config: %s: %s' % (command_options['config_path'], e))
    
  
  # If there are any command args, get them
  command_args = args
  
  # Process the command
  if 1:
  #try:
    # Process the command and retrieve a result
    result = ProcessCommand(config, command_options, command_args)
    
    # Format and output the result (pprint/json/yaml to stdout/file)
    FormatAndOuput(result, command_options)
  
  #NOTE(g): Catch all exceptions, and return in properly formatted output
  #TODO(g): Implement stack trace in Exception handling so we dont lose where this
  #   exception came from, and can then wrap all runs and still get useful
  #   debugging information
  #except Exception, e:
  else:
    utility.error.Error({'exception':str(e)}, command_options)


if __name__ == '__main__':
  #NOTE(g): Fixing the path here.  If you're calling this as a module, you have to 
  #   fix the utility/handlers module import problem yourself.
  sys.path.append(os.path.dirname(sys.argv[0]))

  Main(sys.argv[1:])

