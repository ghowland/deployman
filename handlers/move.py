"""
sysync: handlers: tarball

Module installing from tarballs
"""


from utility.log import Log
from utility.error import Error


def GetKeys(section_item, options):
  """Returns the key used for the Work List/Data work_key"""
  if 'name' not in section_item or section_item['name'] == None:
    Error('Section Item does not have a "name" key: %s' % section_item, options)
  
  # Returns List, always a single item for this handler
  return [section_item['name']]


def Install(section_item, config, options):
  """Will install files from a tarball"""
  Log('Tarball: %s' % section_item)
  
  
