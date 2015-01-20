"""
Utility: Section Handler

Methods for dealing with Section Handlers (elements of Packages)
"""


import os
import logging

import handlers
import configuration
from error import Error
from log import Log


def GetModule(section_handler_name, options):
  """Returns the Python Module for the specified Section Handler"""
  # If we dont have this python script in our handlers module
  if not hasattr(handlers, section_handler_name):
    Error('No Section Handler named: %s' % section_handler_name, options)
  
  # Get the Section Handler python module
  handler = getattr(handlers, section_handler_name)
  
  return handler


def GetDefaults(section_handler_name, options):
  # Get the Section Handler default data
  default_handler_path = '%s/%s.yaml' % (os.path.dirname(options['handler_data_path']), section_handler_name)
  
  # If there is no file, WARN
  if not os.path.isfile(default_handler_path):
    Log('No default Section Handler data found: %s' % default_handler_path, logging.WARN)
    default_section_handler_data = {}
  
  # Else, load the default Section Handler data
  else:
    default_section_handler_data = configuration.LoadYaml(default_handler_path, options)
    
    if type(default_section_handler_data) != dict:
      Error('The default Section Handler data is not in a dictionary format: %s: type: %s' % ())
  
  return default_section_handler_data


def Install(section_handler_name, section_item, host_data, options):
  """Install this section_item, via it's Section Handler script, found through
  the section_handler_name.
  
  host_data is used for templating and options is used for paths and whether
  this is a dry-live or will complete work.
  """
  # If we dont have this python script in our handlers module
  if not hasattr(handlers, section_handler_name):
    Error('No Section Handler named: %s' % section_handler_name, options)
  
  # Get the Section Handler python module
  handler = getattr(handlers, section_handler_name)
  
  # Get the Section Handler default data
  default_handler_path = '%s/%s.yaml' % (os.path.dirname(options['handler_data_path']), section_handler_name)
  
  # If there is no file, WARN
  if not os.path.isfile(default_handler_path):
    Log('No default Section Handler data found: %s' % default_handler_path, logging.WARN)
    default_section_handler_data = {}
  
  # Else, load the default Section Handler data
  else:
    default_section_handler_data = configuration.LoadYaml(default_handler_path, options)
    
    if type(default_section_handler_data) != dict:
      Error('The default Section Handler data is not in a dictionary format: %s: type: %s' % ())
  
  
  # Layer the Section Item data over the Default Section Handler data
  layered_item_data = {}
  layered_item_data.update(default_section_handler_data)
  layered_item_data.update(section_item)
  
  # Use the Section Handler to install this Section Item
  handler.Install(layered_item_data, host_data, options)

