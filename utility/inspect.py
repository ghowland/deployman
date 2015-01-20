"""
sysync: utility: install

Methods for installing packages on a host
"""

import configuration
import install
from error import Error
from log import Log
import run


def InspectHostGroup(host_group, options, args):
  """Inspect a Host Group.  Load and process it's Final Configuration Specification, and return it."""
  host_groups = configuration.GetHostGroups(options)
  
  if host_group not in host_groups:
    Error('Host Group does not exist: %s' % host_group, options)

  host_group_data = host_groups[host_group]
  
  # Create our Master Work List and Spec from our Packages
  (work_list, work_data) = install.CreateMasterWorkListFromPackages(host_group_data.get('packages', []), options)
  
  result = []
  
  # Create a list of the work, in sequence of work-to-be-done
  for key in work_list:
    result.append(work_data[key])
  
  return result
