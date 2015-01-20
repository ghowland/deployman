"""
sysync: handlers: yum

Module installing yum
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
  # If we want to install it
  if section_item['remove'] == False:
    # See if we can check the cache
    #TODO(g): Versions cant be checked.  Split the names, or always take the penalty of trying to install!!!!
    (status, output) = Run('rpm --nofiles --noscripts -V %s' % section_item['name'])
    if status != 0:
      Log('Yum: %s (%s)' % (section_item,output))
  
      RunOnCommit('/usr/bin/yum install -y %s' % section_item['name'], 'Failed to install yum package: %s' % section_item['name'], options)
  
    # Else, already installed
    else:
      Log('Yum: %s [installed]' % section_item)
  
  # Else, we want to remove the package
  else:
    (status, output) = Run('rpm --nofiles --noscripts -V %s' % section_item['name'])
    if status == 0:
      Log('Yum Remove: %s (%s)' % (section_item,output))
  
      RunOnCommit('/usr/bin/yum remove -y %s' % section_item['name'], 'Failed to install yum package: %s' % section_item['name'], options)
      
    # Else, already not installed
    else:
      Log('Yum: %s [not installed]' % section_item)

