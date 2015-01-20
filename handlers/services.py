"""
sysync: handlers: service

Module installing services
"""


from utility.log import Log
from utility.error import Error
from utility.run import Run, RunOnCommit


def GetKeys(section_item, options):
  """Returns the key used for the Work List/Data work_key"""
  if 'name' not in section_item or section_item['name'] == None:
    Error('Section Item does not have a "name" key: %s' % section_item, options)
  
  # Returns List, always a single item for this handler
  return [section_item['name']]


def Install(section_item, config, options):
  Log('Service: %s' % section_item)

  (status, output) = Run('chkconfig --list %s' % section_item['name'])
  if status == 0:
    is_installed = True
  else:
    is_installed = False

  # If we want it to run
  if section_item['remove']:
    if is_installed:
      RunOnCommit('/sbin/chkconfig --del %s' % (section_item['name']), None, options)
  
  else:
    # If this isnt already installed, add it
    if not is_installed and section_item['run']:
      RunOnCommit('/sbin/chkconfig --add %s' % section_item['name'], 'Failed to add to chkconfig services: %s' % section_item['name'], options)
    
    # Split the levels into individual items to get this precisely correct
    for level_str in str(section_item['levels']):
      try:
        level = int(level_str)
      except ValueError, e:
        Error('Service Level must be a string of integers with no spaces.  Invalid character: "%s"' % level_str)
      
      # Check each level and adjust per level
      (status, output) = Run('chkconfig --levels %s %s' % (level, section_item['name']))
      if status == 0:
        level_is_on = True
      else:
        level_is_on = False
      
      # If we want it on, and its off: turn it on
      if section_item['run'] and not level_is_on:
        RunOnCommit('/sbin/chkconfig --level %s %s on' % (level, section_item['name']), 'Failed to enable chkconfig level: %s: %s' % (section_item['name'], level), options)
      
      # Else, if we want it off, and its on: turn if off
      elif not section_item['run'] and level_is_on:
        RunOnCommit('/sbin/chkconfig --level %s %s off' % (level, section_item['name']), 'Failed to disable chkconfig level: %s: %s' % (section_item['name'], level), options)

