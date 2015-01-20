"""
sysync: utility: OS Compatibility

Wrapping OS operations, so they can be dealt with in a system independent 
fashion in the Section Handlers, but can deal with multiple OSes.  As needed.

Best to separate these from Section Handler code.
"""

from run import Run

def GetFileStatInfo(path):
  """Returns (user, group, mode, is_dir) for a given path, or (None, None, None, None) if not found"""
  (status, output) = Run('/bin/ls -ld %s' % path)
  
  # File not found, return all None
  if status != 0:
    return (None, None, None, None)
  
  # Remove list formatting
  while '  ' in output:
    output = output.replace('  ', ' ')
  
  args = output.split(' ')
  #print args
  
  # Initialized local working variables
  (user, group, mode, is_dir) = (None, None, None, False)
  
  user = args[2]
  group = args[3]
  
  # Determine if this is a directory
  if args[0][0] == 'd':
    is_dir = True
  else:
    is_dir = False
  
  # Determine the octal mode from the list formatting, in decimal form
  mode = _GetDecimalModeFromListFormatMode(args[0])
  
  return (user, group, mode, is_dir)


def _GetDecimalModeFromListFormatMode(mode_str):
  """drwxrwxr-x becomes 775.  setuid/sticky-bit are supported and will returns 4 digit string
  
  -rwsrwxrwx becomes 4775
  """
  (user_mode, user_sticky) = _GetDecimalModeFromListFormatMode_Tripplet(mode_str[1:4])
  (group_mode, group_sticky) = _GetDecimalModeFromListFormatMode_Tripplet(mode_str[4:7])
  (any_mode, any_sticky) = _GetDecimalModeFromListFormatMode_Tripplet(mode_str[7:10])
  
  decimal_mode = 0
  
  decimal_mode += user_mode * 100
  decimal_mode += group_mode * 10
  decimal_mode += any_mode
  
  # If we have any sticky bits set
  if user_sticky:
    decimal_mode += 4000
  if group_sticky:
    decimal_mode += 2000
  if any_sticky:
    decimal_mode += 1000
  
  return decimal_mode


def _GetDecimalModeFromListFormatMode_Tripplet(mode_tripplet_str):
  """rwx returns 7, r-x returns 5."""
  decimal_mode = 0
  sticky_bit = False
  
  #print mode_tripplet_str
  
  if mode_tripplet_str[0] == 'r':
    decimal_mode += 4
  if mode_tripplet_str[1] == 'w':
    decimal_mode += 2
  
  if mode_tripplet_str[2] == 'x':
    decimal_mode += 1
  elif mode_tripplet_str[2] == 's':
    decimal_mode += 1
    sticky_bit = True
  
  return (decimal_mode, sticky_bit)


