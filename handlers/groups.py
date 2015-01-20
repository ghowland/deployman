"""
sysync: handlers: groups

Module installing groups
"""


from utility.log import Log
from utility.error import Error
from utility.run import Run, RunOnCommit
from utility.os_compatibility import GetFileStatInfo


# Commands we will use to manage groups
COMMAND_GROUP_EXISTS = 'egrep -i "^%s:" /etc/group'
COMMAND_ADD_GROUP = '/usr/sbin/groupadd'


def GetKeys(section_item, options):
  """Returns the key used for the Work List/Data work_key"""
  if 'name' not in section_item or section_item['name'] == None:
    Error('Section Item does not have a "name" key: %s' % section_item, options)
  
  # Returns List, always a single item for this handler
  return [section_item['name']]


def Install(section_item, config, options):
  Log('Group: %s' % section_item)

  # Does this group exist?
  (status, output) = Run(COMMAND_GROUP_EXISTS % section_item['name'])
  
  # If this group doesnt exist, add it
  if status != 0:
    cmd = COMMAND_ADD_GROUP
    
    # -- Add Options --
    # UID
    if section_item['gid'] != None:
      try:
        int(section_item['gid'])
      except ValueError, e:
        Error('Group GID is not a number: %s: %s' % (section_item['name'], e), options)
      
      cmd += ' -g %s' % section_item['gid']
    
    # Add the group name
    cmd += ' %s' % section_item['name']
    
    RunOnCommit(cmd, 'Failed to add group: %s' % section_item['name'], options)
    
