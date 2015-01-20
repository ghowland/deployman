"""
sysync: handlers: users

Module installing users
"""


import os
import grp


from utility.log import Log
from utility.error import Error
from utility.run import Run, RunOnCommit
from utility.os_compatibility import GetFileStatInfo

import files


# Commands we will use to manage users
COMMAND_USER_EXISTS = '/usr/bin/id'
COMMAND_USER_GROUPS = '/usr/bin/id -G'
COMMAND_ADD_USER = '/usr/sbin/useradd'
COMMAND_SET_NON_PRIMARY_GROUPS = '/usr/sbin/usermod -G'

# Path name for SSH auth keys
SSH_AUTHORIZED_KEYS_NAME = 'authorized_keys'


def GetKeys(section_item, options):
  """Returns the key used for the Work List/Data work_key"""
  if 'name' not in section_item or section_item['name'] == None:
    Error('Section Item does not have a "name" key: %s' % section_item, options)
  
  # Returns List, always a single item for this handler
  return [section_item['name']]


def Install(section_item, config, options):
  """Will add users that do not exist, or remove them.
  
  NOTE(g): Currently will not modify users that exist and have different data.  First things first...
  """
  Log('User: %s' % section_item)
  
  # Groups must be a list, even if its just 1
  if type(section_item['groups']) not in (tuple, list):
    Error('User groups is not a list: %s: %s' % (section_item['name'], section_item['groups']), output)
  
  # Fetch the primary group
  if section_item['groups']:
    primary_group = section_item['groups'][0]
  else:
    primary_group = None
  
  # Does this user exist?
  (status, output) = Run('%s %s' % (COMMAND_USER_EXISTS, section_item['name']))
  
  # If this user doesnt exist, add them
  if status != 0 and section_item['remove'] == False:
    # Start building the command with basics
    if primary_group:
      cmd = """%s --create-home -c '%s' -g '%s'""" % (COMMAND_ADD_USER, section_item['fullname'], primary_group)
    else:
      cmd = """%s --create-home -c '%s'""" % (COMMAND_ADD_USER, section_item['fullname'])
    
    # -- Add Options --
    
    # UID
    if section_item['uid'] != None:
      try:
        int(section_item['uid'])
      except ValueError, e:
        Error('User UID is not a number: %s: %s' % (section_item['name'], e), options)
      
      cmd += ' -u %s' % section_item['uid']

    # Shell
    if section_item['shell'] != None:
      if not os.path.isfile(section_item['shell']):
        Error('User shell is not an existing file: %s: %s' % (section_item['name'], section_item['shell']), options)
      
      cmd += ' -s %s' % section_item['shell']

    # Home Directory
    if section_item['home'] != None:
      cmd += ' -d "%s"' % section_item['home']
    
    # Crypted Password
    if section_item['crypt_password'] != None:
      cmd += ' -p "%s"' % section_item['crypt_password']
    
    # Add the username
    cmd += ' %s' % section_item['name']
    
    RunOnCommit(cmd, 'Failed to add user: %s' % section_item['name'], options)
  
  # If we want this user to exist, do more checks about them
  if section_item['remove'] == False:
    # Get the user's home directory using shell expansion
    (_, home_path) = Run('/bin/echo ~%s' % section_item['name'])
    
    # Get the current user, group, mode, is_dir
    (cur_user, cur_group, cur_mode, cur_is_dir) = GetFileStatInfo(home_path)
    
    #TODO(g): Enforce the user owns their home directory?  Any problems here?  Think about it...
    pass
    
    # Test Primary Group is correct
    #TODO(g): Do this...
    pass
    
    # Test Full Name is correct
    #TODO(g): Do this...
    pass
    
    # Additional groups, none by default
    current_additonal_groups = []
    additional_groups = section_item['groups'][1:]
    cmd = '%s %s' % (COMMAND_USER_GROUPS, section_item['name'])
    (group_status, group_output) = Run(cmd)
    # If we got a valid group output (failure gives us bad data)
    if group_status == 0:
      current_additonal_group_ids = group_output.split(' ')[1:]
      # Convert from groups IDs to group names to test
      for current_additonal_group_id in current_additonal_group_ids:
        try:
          current_additonal_groups.append(grp.getgrgid(int(current_additonal_group_id))[0])
        except ValueError, e:
          Error('Invalid group id: %s (from list: %s)' % (current_additonal_group_id, current_additonal_group_ids), options)
    
    current_additonal_groups.sort()
    additional_groups.sort()
  
    # If our additonal groups arent the same, set them all
    if additional_groups != current_additonal_groups:
      #NOTE(g): The empty set works here.  It will clear non-primary groups.
      cmd = '%s "%s" %s' % (COMMAND_SET_NON_PRIMARY_GROUPS, ','.join(additional_groups), section_item['name'])
      RunOnCommit(cmd, 'Failed set non-primary groups for user: %s: %s' % (section_item['name'], str(additional_groups)), options)
      
    
    # SSH Key
    if section_item['ssh key'] != None:
      # Get the current user, group, mode, is_dir
      ssh_path = '%s/.ssh' % home_path
      (cur_user, cur_group, cur_mode, cur_is_dir) = GetFileStatInfo(ssh_path)
      
      # If the SSH path doesnt exist, create it properly
      if cur_user == None:
        RunOnCommit('/bin/mkdir %s' % ssh_path, 'Failed to make SSH dir for user: %s: %s' % (section_item['name'], ssh_path), options)
        if primary_group:
          RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], primary_group, ssh_path), 'Failed to chown SSH path: %s: %s' % (section_item['name'], ssh_path), options)
        else:
          RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], section_item['name'], ssh_path), 'Failed to chown SSH path: %s: %s' % (section_item['name'], ssh_path), options)
        RunOnCommit('/bin/chmod 700 %s' % ssh_path, 'Failed to chmod SSH path: %s: %s' % (section_item['name'], ssh_path), options)
        
      # Else, enforce it is properly set up
      else:
        # If the user isnt the owner of the SSH directory, fix it
        if cur_user != section_item['name']:
          if primary_group:
            RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], primary_group, ssh_path), 'Failed to chown SSH path: %s: %s' % (section_item['name'], ssh_path), options)
          else:
            RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], section_item['name'], ssh_path), 'Failed to chown SSH path: %s: %s' % (section_item['name'], ssh_path), options)
      
        # If the mode is wrong, fix it
        if cur_mode != 700:
          RunOnCommit('/bin/chmod 700 %s' % ssh_path, 'Failed to chmod SSH path: %s: %s' % (section_item['name'], ssh_path), options)
      
      
      # If the SSH Auth Keys file does not exist, create it with the key and set permissions
      ssh_auth_keys_path = '%s/%s' % (ssh_path, SSH_AUTHORIZED_KEYS_NAME)
      if not os.path.isfile(ssh_auth_keys_path):
        cmd = """echo %s > %s""" % (section_item['ssh key'], ssh_auth_keys_path)
        RunOnCommit(cmd, 'Failed create SSH Authorized Keys file: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)
        if primary_group:
          RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], primary_group, ssh_auth_keys_path), 'Failed to chown SSH Auth Keys path: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)
        else:
          RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], section_item['name'], ssh_auth_keys_path), 'Failed to chown SSH Auth Keys path: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)

        RunOnCommit('/bin/chmod 600 %s' % ssh_auth_keys_path, 'Failed to chmod SSH Auth Keys path: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)
      
      # Else, it exists, test the permissions and that the SSH auth key exists
      else:
        # Test for the existing key
        (_, found_key) = Run("""grep '%s' %s""" % (section_item['ssh key'], ssh_auth_keys_path))
        if not found_key:
          # Append the current SSH key to the existing file
          #TODO(g): Is it ok to leave the old keys?  Will be inconsistent if we blow it away, but leaves old keys around...  Global option for this?
          cmd = """echo %s >> %s""" % (section_item['ssh key'], ssh_auth_keys_path)
          RunOnCommit(cmd, 'Failed create SSH Authorized Keys file: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)

        # Test permissions on the SSH Auth Key file
        (cur_user, cur_group, cur_mode, cur_is_dir) = GetFileStatInfo(ssh_auth_keys_path)
        
        # If the user isnt the owner of the SSH directory, fix it
        if cur_user != section_item['name']:
          if primary_group:
            RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], primary_group, ssh_auth_keys_path), 'Failed to chown SSH Auth Key path: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)
          else:
            RunOnCommit('/bin/chown %s:%s %s' % (section_item['name'], section_item['name'], ssh_auth_keys_path), 'Failed to chown SSH Auth Key path: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)
      
        # If the mode is wrong, fix it
        if cur_mode != 600:
          RunOnCommit('/bin/chmod 600 %s' % ssh_auth_keys_path, 'Failed to chmod SSH Auth Key path: %s: %s' % (section_item['name'], ssh_auth_keys_path), options)

