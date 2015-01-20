"""
sysync: handlers: files

Module installing files
"""


import os

from utility.log import Log
from utility.error import Error
from utility.run import Run, RunOnCommit
from utility.os_compatibility import GetFileStatInfo


def GetKeys(section_item, options):
  """Returns the key used for the Work List/Data work_key"""
  if 'path' not in section_item or section_item['path'] == None:
    Error('Section Item does not have a "path" key: %s' % section_item, options)
  
  # Collect all the files effected in the files Section Handler, which could
  #   be many files, or could just be the main path/directory.  Will not
  #   test existance or non-existance, only that some processing will happen
  #   on that specific path/file.
  files = []

  # If a "source path" is specified, we will look through it for keys
  if 'source path' in section_item and section_item['source path'] != None:
    # No symlink instruction
    if 'symlink path' in section_item and section_item['symlink path'] != None:
      Error('Section Item cannot have both "source path" and "symlink path" keys, they are mutually exclusive: %s' % section_item, options)
    
    # No remove instruction
    if 'remove' in section_item and section_item['remove'] not in (None, False):
      Error('Section Item cannot have both "source path" and "remove" keys, they are mutually exclusive: %s' % section_item, options)
    
    # Handle this case when copying from a "source path" as well
    #NOTE(g): The base directory must be handled before the files, so we can 
    #   ensure it exists properly before putting files in it.  
    #   Note that if a previous section lists these files first, and doesnt 
    #   specify setting the directory permissions, then the directory 
    #   processing will be done after the files, but the directory will still
    #   be created properly and use the correct permissions, after it has been
    #   created with a default.
    #   In this case double-work is done, because I do not want to do
    #   heavier processing and require data sharing to make this always work
    #   no matter what order the statements are given in.  If it is important,
    #   use proper ordering to get the effect you desire.
    if 'set base directory permissions' in section_item and section_item['set base directory permissions'] not in (None, False):
      if section_item['path'] not in files:
        files.append(section_item['path'])
        Log('Adding path: %s' % section_item['path'])
    
    # Get all the files that 
    files += _CollectDestFilesFromSourcePath(section_item, options)
  
  # Symlink the specified path
  elif 'symlink path' in section_item and section_item['symlink path'] != None:
    # No remove instruction
    if 'remove' in section_item and section_item['remove'] not in (None, False):
      Error('Section Item cannot have both "symlink path" and "remove" keys, they are mutually exclusive: %s' % section_item, options)
  
    files.append(section_item['path'])
    Log('Adding path: %s' % section_item['path'])
  
  # Remove the specified path
  elif 'remove' in section_item and section_item['remove'] not in (None, False):
    files.append(section_item['path'])
    Log('Adding path: %s' % section_item['path'])
  
  # Set base directory permissions
  elif 'set base directory permissions' in section_item and section_item['set base directory permissions'] not in (None, False):
    files.append(section_item['path'])
    Log('Adding path: %s' % section_item['path'])
  
  # Else, enforce permissions
  else:
    files.append(section_item['path'])
    Log('Adding path: %s' % section_item['path'])
  
  
  # #DEBUG
  # label = section_item.get('source path', None)
  # if label != None:
  #   print 'File Collect: %s - %s'  % (label, files)
  # else:
  #   print 'File Collect: %s - %s'  % (section_item, files)
  
  return files


def _CollectDestFilesFromSourcePath(section_item, options):
  """Install files from a Source Path.  The most common method of using the file handler.
  
  Returns: list of strings
  """
  # Result, all the files in the Source Path
  result_files = []
  
  # Modify the source_path with the option
  section_item['source path'] = '%s/%s' % (options['files_path'], section_item['source path'])
  
  # Walk through the files
  for (dir_path, directories, files) in os.walk(section_item['source path']):
    # Store the remains of this path, that gets added to section_item['path']
    path_remains = dir_path[len(section_item['source path']):]
    
    # Check for directories we dont want to copy
    if 'ignore directories' in section_item:
      # Enfore List format
      if type(section_item['ignore directories']) not in (list, tuple):
        Error('"ignore dirs" directive is not a List: %s' % section_item, options)
    
      # Look for skipped dirs
      skip_this_directory = False
      for ignore_dir in section_item['ignore directories']:
        ignore_path =  '/%s/' % ignore_dir
        if ignore_path in dir_path or dir_path.endswith('/%s' % ignore_dir):
          #Log('Ignore test: %s -- %s' % (ignore_path, dir_path))
          skip_this_directory = True
          break
    
      if skip_this_directory:
        #Log('Skipping directory: %s' % dir_path)
        continue
  
  
    # Process the files
    for filename in files:
      # Check for directories we dont want to copy
      if 'ignore files' in section_item:
        # Enfore List format
        if type(section_item['ignore files']) not in (list, tuple):
          Error('"ignore files" directive is not a List: %s' % section_item, options) 
      
        # Look for skipped dirs
        skip_this_file = False
        for ignore_file in section_item['ignore files']:
          if ignore_file == filename:
            #Log('Ignore test: %s == %s' % (ignore_file, filename))
            skip_this_file = True
            break
      
        if skip_this_file:
          continue
      else:
        Log('No ignore files...')
    
      # Create the destination path
      dest_path = '%s/%s' % (os.path.dirname(section_item['path']), path_remains)
      
      # Create the full destination path, which is what we want to track
      dest = '%s/%s' % (dest_path, filename)
      
      # Clean up the double slashes
      #TODO(g): Enforce that this never happens by clean data processing above
      while '//' in dest:
        dest = dest.replace('//', '/')
      
      # Save our dest file, which is what we will be operating on.  These and only these.
      if dest not in result_files:
        result_files.append(dest)
  
  # If we didnt collect files from "source path"
  if not result_files:
    # If the source path is a single file, and it exists, that is the target
    if os.path.isfile(section_item['source path']):
      path = section_item['path'].replace('//', '/')
      result_files.append(path)
    else:
      Error('"source path" contains no files to process and is not a file itself, invalid configuration: %s' % section_item, options)
  
  # Returns the collection of files from the source_path, but as their destination paths
  return result_files


def Install(section_item, config, options):
  #Log('Files: Install: %s -- %s -- %s' % (section_item['__key'], section_item, config))
  
  # If a "source path" is specified, we will look through it for keys
  if 'source path' in section_item and section_item['source path'] != None:
    # No symlink instruction
    if 'symlink path' in section_item and section_item['symlink path'] != None:
      Error('Section Item cannot have both "source path" and "symlink path" keys, they are mutually exclusive: %s' % section_item, options)
    
    # No remove instruction
    if 'remove' in section_item and section_item['remove'] not in (None, False):
      Error('Section Item cannot have both "source path" and "remove" keys, they are mutually exclusive: %s' % section_item, options)
    
    # Handle this case when copying from a "source path" as well
    #NOTE(g): The base directory must be handled before the files, so we can 
    #   ensure it exists properly before putting files in it.  
    #   Note that if a previous section lists these files first, and doesnt 
    #   specify setting the directory permissions, then the directory 
    #   processing will be done after the files, but the directory will still
    #   be created properly and use the correct permissions, after it has been
    #   created with a default.
    #   In this case double-work is done, because I do not want to do
    #   heavier processing and require data sharing to make this always work
    #   no matter what order the statements are given in.  If it is important,
    #   use proper ordering to get the effect you desire.
    if 'set base directory permissions' in section_item and section_item['set base directory permissions'] not in (None, False):
      Log('Setting base directory permissions')
      EnforceDirectory(section_item['path'], '', section_item, options)
    
    # Determine the file_path by stripping the install path from the dest __key
    file_path = section_item['__key'][len(section_item['path']):]
    
    # Determine the source file from the key and the source path
    #TODO(g): Handle "template" variable here, which would point it at a new temporary source path...
    if file_path:
      source_file = '%s/%s' % (section_item['source path'], file_path)
    else:
      source_file = section_item['source path']
    
    # Enforce the file
    if not source_file.endswith('/'):
      # If we are not templating this file, then do a normal Enforce
      if section_item['template'] == False:
        EnforceFile(source_file, section_item['__key'], section_item, options)
      
      # Else, we are templating this file, so handle that
      else:
        # Create the temp file for this
        template_source_file = '%s/%s' % (options['deploy_temp_path'], section_item['__key'])
        Log('Creating temporary file for templating: %s' % template_source_file)
        
        # Ensure temp directory up to file exists
        Run('/bin/mkdir -p %s' % os.path.dirname(template_source_file))
        
        # Template the file contents
        #TODO(g): Move this into its own function after it works, to clean this up.  It should be streamlined at this point, its complex enough...
        source_file_contents = open(source_file).read()
        for (template_key, template_value) in config['host_group'].get('data', {}).items():
          template_var = '%%(%s)s' % template_key
          if template_var in source_file_contents:
            # If this isnt a list or tuple, then do standard string template replacement
            if type(template_value) not in (list, tuple):
              source_file_contents = source_file_contents.replace(template_var, str(template_value))
            # Else, do a modulus of this hosts position in the host list
            else:
              # Get the index of this host in it's Host Group's hosts
              index_position = config['host_group']['hosts'].index(host_data['hostname'])
              # Determine the template index by taking the modulus of the index position by the number of template values
              template_index = index_position % len(template_value)
              # Replace the template variable with the modulus index of the template list/tuple
              source_file_contents = source_file_contents.replace(template_var, str(template_value[template_index]))
            
            # Save the templated source file
            open(template_source_file, 'w').write(source_file_contents)
        
        # Enforce from the templated source file
        EnforceFile(template_source_file, section_item['__key'], section_item, options)
    
    # Else, Enforce the dir
    else:
      EnforceDirectory(section_item['path'], '', section_item, options)
  
  # Symlink the specified path
  elif 'symlink path' in section_item and section_item['symlink path'] != None:
    # No remove instruction
    if 'remove' in section_item and section_item['remove'] not in (None, False):
      Error('Section Item cannot have both "symlink path" and "remove" keys, they are mutually exclusive: %s' % section_item, options)
  
    SymlinkPath(section_item, options)
  
  # Remove the specified path
  elif 'remove' in section_item and section_item['remove'] not in (None, False):
    RemoveFile(section_item['path'], options)
  
  # Set base directory permissions
  elif 'set base directory permissions' in section_item and section_item['set base directory permissions'] not in (None, False):
    if section_item['path'].endswith('/'):
      EnforceDirectory(section_item['path'], '', section_item, options)
    else:
      Error('Section Item path must end in a trailing slash (/dir/) when using "set base directory permissions" and no "source path": %s' % section_item, options)
  
  # Else, enforce permissions
  else:
    if section_item['path'].endswith('/'):
      EnforceDirectory(section_item['path'], '', section_item, options)
    else:
      EnforceFilePermissions(section_item['path'], section_item, options)


def EnforceFile(source, dest, section_item, options):
  """Enforce that this file is on the system at dest."""
  copy_file = False
  
  #Log('Enforce File Start: %s -- %s' % (source, section_item))
  
  # If the file path doesnt exist
  if not os.path.exists(dest):
    copy_file = True
    Log('Enforce File: %s --> %s (not found)' % (source, dest))
    
    # If the directory doesnt exist, enforce it
    if not os.path.exists(os.path.dirname(dest)):
      Log('Enforce File Parent Directory: %s :: %s (not found)' % (dest, os.path.dirname(dest)))
      EnforceDirectory(os.path.dirname(dest), '', section_item, options)
  
  # Else, the path exists, check the content
  else:
    # Get the current md5sum (None if doesnt exit)
    (source_status, source_output) = Run("/usr/bin/md5sum %s | awk '{ print $1 }'" % source)
    (dest_status, dest_output) = Run("/usr/bin/md5sum %s | awk '{ print $1 }'" % dest)
    
    # Compare md5sums
    if (dest_output != source_output):
      copy_file = True
      Log('Enforce File: %s --> %s (content changed) %s != %s' % (source, dest, source_output, dest_output))
    else:
      Log('Enforce File: %s --> %s (content the same) %s == %s' % (source, dest, source_output, dest_output))


  # If we have the special directory "if not match", which will invalidate this section
  #NOTE(g): This is the only place I perform an exclusionary match, and is reverse case of the "ordered" argument
  if copy_file and section_item['if not match'] != None:
    # Get the current md5sum (None if doesnt exit)
    (source_status, source_output) = Run("/usr/bin/md5sum %s | awk '{ print $1 }'" % section_item['if not match'])
    (dest_status, dest_output) = Run("/usr/bin/md5sum %s | awk '{ print $1 }'" % dest)
    
    # Compare md5sums
    if (dest_output == source_output):
      copy_file = False
      Log('Enforce File REJECT: %s --> %s ("if not match" clause) %s == %s' % (section_item['if not match'], dest, source_output, dest_output))
    

  # Clean up the double slashes
  #TODO(g): Enforce that this never happens by clean data processing above
  while '//' in source:
    source = source.replace('//', '/')
  while '//' in dest:
    dest = dest.replace('//', '/')
  
  # If we need to copy the source to dest
  if copy_file:
    RunOnCommit('/bin/cp %s %s' % (source, dest), 'Failed to copy file to: %s' % dest, options)
  
  # Enforce destination File Permissions
  EnforceFilePermissions(dest, section_item, options)


def EnforceFilePermissions(path, section_item, options):
  """Enforce that this file is on the system at dest."""
  user = section_item['user']
  group = section_item['group']
  mode = section_item['mode']
  
  # Get the current user, group, mode, is_dir
  (cur_user, cur_group, cur_mode, cur_is_dir) = GetFileStatInfo(path)
  
  # If either the user or group is not as specified, change
  if (cur_user != user or cur_group != group):
    Log('Enforce File Ownership: %s (%s:%s) - Current: (%s:%s)' % (path, user, group, cur_user, cur_group), print_stack=4)
    RunOnCommit('/bin/chown -h %s:%s %s' % (user, group, path), 'Failed to set user/group: %s:%s %s' % (user, group, path), options)
  
  # If the mode is not as specified, change
  if (cur_mode != mode):
    Log('Enforce File mode: %s (%s) - Current: %s' % (path, mode, cur_mode))
    RunOnCommit('/bin/chmod %s %s' % (mode, path), 'Failed to change mode: %s %s' % (mode, path), options)


def EnforceDirectory(base_path, enforce_path, section_item, options):
  """Enfore the directory exists."""
  user = section_item['user']
  group = section_item['group']
  mode = section_item['directory mode']
  
  Log('Enforce Directory: %s/%s (%s:%s %s)' % (base_path, enforce_path, user, group, mode))

  # Parts of the path
  parts = enforce_path.split('/')
  
  # Test all paths in this range
  for count in range(1, len(parts)+1):
    active_path = parts[:count]
    
    test_path = '%s/%s' % (base_path, '/'.join(active_path))
    
    # Clean up the double slashes
    #TODO(g): Enforce that this never happens by clean data processing above
    while '//' in test_path:
      test_path = test_path.replace('//', '/')
    
    if os.path.isfile(test_path):
      Error('An install directory is actually a file, fix this manually: %s: %s' % (dest_path, section_item), options)
    
    # Create it if it doesnt exist
    if not os.path.isdir(test_path):
      _CreateDirectory(test_path, section_item, options)
    
    else:
      # Compare ownership
      (current_user, current_group, current_mode, current_is_dir) = GetFileStatInfo(test_path)
    
      if current_user != user or current_group != group:
        Log('Enforce Dir Ownership: %s (%s:%s) - Current: (%s:%s)' % (test_path, user, group, current_user, current_group))
        RunOnCommit('/bin/chown %s:%s %s' % (user, group, test_path), 'Failed to set user/group: %s:%s %s' % (user, group, test_path), options)

      if current_mode != mode:
        Log('Enforce Dir mode: %s (%s) - Current: %s' % (test_path, mode, current_mode))
        RunOnCommit('/bin/chmod %s %s' % (mode, test_path), 'Failed to change mode: %s %s' % (mode, test_path), options)


def _CreateDirectory(path, section_item, options):
  """Create a specified directory"""
  # Determine the user/group
  user = section_item['user']
  group = section_item['group']
  mode = section_item['directory mode']
  
  # If the path does not exist
  if not os.path.exists(path):
    Log('Create Directory: %s (%s %s:%s)' % (path, mode, user, group))
    
    RunOnCommit('/bin/mkdir -p %s' % path, 'Failed to make directory: %s' % path, options)
    RunOnCommit('/bin/chown %s:%s %s' % (user, group, path), 'Failed to set user/group: %s:%s %s' % (user, group, path), options)
    RunOnCommit('/bin/chmod %s %s' % (mode, path), 'Failed to change mode: %s %s' % (mode, path), options)
  
  else:
    Log('Skipping Create Directory: %s (%s %s:%s) -- Already exists' % (path, mode, user, group))


def SymlinkPath(section_item, options):
  """Symlink the specified path to the target path."""
  target_path = section_item['path']
  symlink_path = section_item['symlink path']
  
  Log('SymLink Path: %s -> %s' % (symlink_path, target_path))
  
  # Crop the trailing slash, as it messes up the comparison
  if symlink_path.endswith('/'):
    symlink_path = symlink_path[:-1]
  
  # If no file exists, or its a different symlink
  if not os.path.exists(target_path) or (os.path.islink(target_path) and os.path.realpath(target_path) != symlink_path):
    Log('SymLink Path Data: %s - %s' % (os.path.islink(target_path), os.path.realpath(target_path)))
    RunOnCommit('/bin/ln -sf %s %s' % (symlink_path, target_path), 'Failed to symlink path: %s to path: %s' % (symlink_path, target_path), options)
  
  # Else, it exists and is correct, do nothing
  elif os.path.exists(target_path) and os.path.islink(target_path) and os.path.realpath(target_path) == symlink_path:
    pass
  
  # Else, error.  Symlinks are an edge case when it's a file or directory and likely to be wrong
  else:
    Error('An existing non-symlink file exists, please clean it up so symlinking can update: %s' % target_path, options)
  
  # Compare ownership
  (current_user, current_group, current_mode, current_is_dir) = GetFileStatInfo(target_path)
  user = section_item['user']
  group = section_item['group']
  
  if current_user != user or current_group != group:
    RunOnCommit('/bin/chown --no-dereference %s:%s %s' % (user, group, target_path), 'Failed to set user/group: %s:%s %s' % (user, group, target_path), options)


def RemoveFile(path, options):
  """Remove the specified path."""
  Log('Remove File: %s' % (path))
  
  # If the file exists
  if os.path.exists(path):
    RunOnCommit('/bin/rm %s' % path, 'Failed to remove file: %s' % path, options)
  else:
    Log('File specified for removal does not exist: %s' % path)

