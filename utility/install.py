"""
sysync: utility: install

Methods for installing packages on a host
"""


import json


import localhost
import configuration
import section_handler
from error import Error
from log import Log
import run


def InstallSystem(config, deployment, options):
  """Install the local host from the sysync deployment configuration files."""
  installed = {}
  
  # Create fresh temporary directory
  Log('Clearing temporary deployment path: %s' % config['deploy_temp_path'])
  run.Run('/bin/rm -rf %s' % config['deploy_temp_path'])
  run.Run('/bin/mkdir -p %s' % config['deploy_temp_path'])
  
  # Install the packages
  result = InstallPackagesLocally(config, deployment, options)
  
  return result


def InstallPackagesLocally(config, deployment, options):
  """Install the packages locally for the specified """
  # Ensure we are starting with a fresh commit list
  run.ClearRunCommitList()
  
  # Work List gives us sequence precidence for work, and Work Data has 
  #   finalized options for each area
  #NOTE(g): work_list is a sequence of keys for work_data, key format: 
  #   "handler:::key"
  #   example: "files:::/etc/resolv.conf"
  (work_list, work_data) = CreateMasterWorkListFromPackages(config, deployment, options)
  
  
  # Process each work item via it's specified Section Handler, in sequence
  for work_key in work_list:
    # Get the item data
    item_data = work_data[work_key]
    
    # Take the Section Handler name from the work key
    section_handler_name = work_key.split(':::')[0]
    
    # Process this work item: registering what commands will be run on Commit
    ProcessWorkItem(section_handler_name, item_data, config, options)


  # If we want to commit this set of changes
  if deployment['command'] == 'commit':
    log('Commiting commands')
    
    # Run each of the commands our handlers specified for commit, and handle any errors
    run.Commit_Commands(config, options)
  
  
  # Return the commit list, we were performing
  return run.RUN_COMMIT_LIST


def ProcessWorkItem(section_handler_name, section_item, config, options):
  """Process each work item individually, registering all commands that should 
  be run on commit.
  """
  #print 'Work: %s: %s' % (section_handler_name, section_item)
  
  # Get the Section Handler python module
  handler_module = section_handler.GetModule(section_handler_name, options)
  
  # Register this Section Handler Work Item for Installation
  handler_module.Install(section_item, config, options)


def CreateMasterWorkListFromPackages(config, deployment, options):
  """Returns (work_list, work_data).  
    work_list (list) is a sequence of keys for work_data (dict), key format:
      'handler:::key' example: 'files:::/etc/resolv.conf'
  """
  input_sections = json.loads(deployment['input_data_json'])
  
  # Initialize our master Work List and Work Data containers
  work_list = []
  work_data = {}
  
  if type(input_sections) not in (list, tuple):
    Error('Package data is incorrect formatted.  Package sections must be in the form of a List for determinism. type: %s' % type(input_sections))

  # Process the package sections
  for section_count in range(0, len(input_sections)):
    section = input_sections[section_count]
  
    # Enforce we have list of dicts
    if type(section) != dict:
      Error('Package Section data is incorrect formatted.  Package Section Data must be in the form of a Dictionary to specify the Section Handler: section number: %s  type: %s' % (section_count, type(section)))
    # Enforce each dict has a single key, to specify the Section Handler
    if len(section.keys()) != 1:
      Error('Package Section data is incorrect formatted.  Package Section Data dictionary must have only 1 key, which specifies the Section Handler: section number: %s  keys: %s' % (section_count, section.keys()))
  
    # Get the section handler
    section_handler_name = section.keys()[0]
  
    # Get the list the Section Handler will process (in order)
    section_list = section[section_handler_name]
  
    # Enforce Section List is a list
    if type(section_list) not in (list, tuple):
      Error('The Package Section list is not in a List format.  Section Handlers process their items in a list, for determinism.  section number: %s  section handler: %s  type: %s' % (section_count, section_handler_name, type(section_list)))
    
    # Install each Package Section Item, via it's Section Handler
    for section_item in section_list:
      # Enforce the Section Item is a dictionary
      if type(section) != dict:
        Error('Section Item is not a dictionary: section number: %s  section handler: %s  type: %s' % (section_count, section_handler_name, type(section)))
      
      # Get the Section Handler python module
      handler_module = section_handler.GetModule(section_handler_name, options)
      
      # Layer section_item over defaults to get this item_data
      #NOTE(g): item_data will be the final data used.  The last update always trumps any 
      #   previous section item, because that is the only way data can retain integrity,
      #   as layering with previous data could create invalid specifications.  The 
      #   last update on a given handler:::key always wins.
      item_data = {}
      handler_defaults = section_handler.GetDefaults(section_handler_name, options)
      try:
        item_data.update(handler_defaults)
      except Exception, e:
        Error('Failed to update handler defaults, improper format (should be dict): %s' % handler_defaults, options)
        
      try:
        item_data.update(section_item)
      except Exception, e:
        Error('Failed to update section items, improper format (should be dict): %s' % section_item, options)
      
      # Get all the Section Handler keys that this section item refers to
      #NOTE(g): Many files could be effected by a singles "files" handler 
      #   section_item, but each file will get the same section_item data
      #   updated.  We want to specify each unit of work individually
      #   so that we have complete precision
      section_handler_keys = handler_module.GetKeys(item_data, options)
      
      # Process all Section Handler Keys
      for section_handler_key in section_handler_keys:
        # Make a new copy of the data for this specific key
        key_data = dict(item_data)
        
        # Create the Work Key, which we use to track data order in Work List, and data in Work Data
        work_key = '%s:::%s' % (section_handler_name, section_handler_key)
        
        # Always keep this accessible in the data.  The key may be needed to figure
        #   out where the work must be done, such as in the 'files' Section Handler
        #   which has data to find the keys, but often not the key itself, and so
        #   needs this specified here.
        key_data['__key'] = section_handler_key
        
        # If this is supposed to be ordered, and not layered
        if key_data.get('ordered', False):
          order_id = len(work_list) + 1
          ordered_work_key = '%s:::%s' % (work_key, order_id)
          work_data[ordered_work_key] = key_data
          work_list.append(ordered_work_key)
          #print 'Ordered: %s' % ordered_work_key
          
        # Else, If we havent seen this work_key before, or it is specified as Ordered, put section_item data over defaults
        elif work_key not in work_list:
          # Add to sequence and data pool
          work_list.append(work_key)
          work_data[work_key] = key_data
        
        # Else, we already have this work_key, so are just updating the item data
        else:
          work_data[work_key] = key_data
  
  #DEBUG
  print 'Work List: %s\nWork Data:' % work_list
  import pprint
  pprint.pprint(work_data)
  
  return (work_list, work_data)

