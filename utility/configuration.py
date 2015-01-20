"""
sysync: utility: configuration

Methods for dealing with sysync configuration files.
"""


import yaml
import glob
import os

from error import Error
from log import Log


def GetHostGroups(options):
  """Returns a dict of host groups, keyed on the host group name."""
  host_groups = {}
  
  # Create a glob path for YAML files
  glob_path = '%s/*.yaml' % os.path.dirname(options['hostgroup_path'])
  
  # Get the YAML files in the Host Group path
  yaml_files = glob.glob(glob_path)
  
  for yaml_file in yaml_files:
    #Log('Loading Host Group: %s' % yaml_file)
    
    # Get the host group name from the YAML file name
    host_group = os.path.basename(yaml_file)[:-5]
    
    # Load the data and assign into the host group dict
    try:
      host_groups[host_group] = LoadYaml(yaml_file, options)
    except Exception, e:
      Error('Couldnt load YAML file: %s: %s' % (yaml_file, e))
  
  return host_groups


def GetHostInHostGroups(hostname, options):
  """Returns a list of strings, host group names that the hostname was found in.
  
  Hostnames are fully qualified domain names (FQDN), which are tested against a 
  Host Group's hosts' name and domain, which are combined to test against the
  hostname.
  """
  host_group_list = []
  
  # Get our host groups
  host_groups = GetHostGroups(options)
  
  # If there arent any host groups, we cant go on
  if not host_groups:
    Error('No host groups were found.  Please add some to /etc/sysync/host_groups/ or use the --hostgroups=directory to specify the desired location for Host Group YAML files.', options)
  
  # Verbose failure message
  verbose_failure = ''
  
  # Look through the host group data for this hostname (host+domain)
  for (hostgroup, data) in host_groups.items():
    # Get the domain, if it is specified
    domain = data.get('domain', '')
    if not domain:
      Log('No domain specified in host group file: %s.yaml' % hostgroup)
    
    if options['verbose']:
      verbose_failure += '   Testing host group: %s\n' % hostgroup
    
    # Try to find a match for this hostgroup
    for host in data.get('hosts', []):
      if domain:
        this_fqdn = '%s.%s' % (host, domain)
      else:
        this_fqdn = host
      
      # If we want verbosity, explain this in great detail
      if options['verbose']:
        verbose_failure += '   Machine Hostname: %s   Test FQDN: %s   Test Host Group:  %s\n' % (hostname, this_fqdn, hostgroup)
      
      # If we found a match for this hostname in this host group, then 
      #   add it to our list, and stop checking hosts in this group
      if this_fqdn == hostname:
        host_group_list.append(hostgroup)
        Log('Matched: %s -> %s' % (hostname, hostgroup))
        break
      else:
        #Log('No match: %s !-> %s' % (hostname, hostgroup))
        pass
  
  
  # Log verbose failure, if we have them
  if not host_group_list and verbose_failure:
    Log('Failed to find any host groups for this host.  Here was the process:\n%s' % verbose_failure)
  
  # Else, log them if verbose
  elif options['verbose']:
    Log('Host Group List: %s' % host_group_list)
  
  return host_group_list


def GetHostData(hostname, host_group, options):
  """Returns a dictionary of host template data."""
  data = {}
  
  # Get the host group data
  host_group_data = GetHostGroups(options)[host_group]
  
  # Data section
  data_section = host_group_data.get('data', {})
  if type(data_section) != type({}):
    Error('Host Group Data section is not a dictionary: %s' % host_group)
  
  # Update our data with the host group template data
  data.update(data_section)
  
  # Ensure the hostname, host_group and domain are always set from logic
  data['hostname'] = hostname.split('.')[0]
  data['fqdn'] = hostname
  data['host_group'] = host_group
  data['domain'] = host_group_data.get('domain', '')
  
  return data


def GetPackage(name, options):
  """Returns the Package Data, or calls Error() if package does not exist."""
  path = '%s/%s.yaml' % (options['package_path'], name)
  
  if not os.path.isfile(path):
    Error('Package does not exist: %s' % path, options)
  
  package_data = LoadYaml(path, options)
  
  return package_data


def LoadYaml(path, options):
  """Wrapper to contain which modules import the yaml module."""
  try:
    data = yaml.load(open(path))
  except yaml.scanner.ScannerError, e:
    Error('Failed to parse YAML: %s: %s' % (path, e), options)
  
  return data


