"""
sysync: utility: format

Methods for formatting data for errors and output
"""


import sys
import pprint
import yaml
import json


def FormatAndOuput(data, options):
  """Format and output the result (pprint/json/yaml to stdout/file)"""
  #TODO(g): Format with data or whatever...  Output to file or whatever...  Whatever...
  if options.get('format', None) == 'json':
    output = json.dumps(data)
  elif options.get('format', None) == 'yaml':
    output = yaml.safe_dump(data)
  elif type(data) == dict:
    # Wrap the top level of dicts on a key per block basis, then pretty print to clean it up
    output = ''
    for key in data:
      output += '%s: %s\n\n' % (key, pprint.pformat(data[key]).replace("\\'", "'"))
  elif type(data) == list:
    # Wrap the top level of dicts on a key per block basis, then pretty print to clean it up
    output = ''
    for item in data:
      output += '%s\n\n' % pprint.pformat(item)
  else:
    output = pprint.pformat(data)
  
  # If we are outputting to a file...
  if 'output' in options:
    try:
      fp = open(options['output'], 'w')
      fp.write(output)
      fp.close()
    except Exception, e:
      #NOTE(g): Cant call Error(), because Error calls this...
      print 'error: Failed to write output to: %s: %s' % (options['output'], e)
      
      # Output the data to STDOUT, as a last resort
      print 'Output:'
      print data
      sys.exit(1)
  
  # Else, output to STDOUT
  else:
    print output


