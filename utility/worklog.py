"""
sysync: utility: worklog

What was done must be able to be undone.  This module's purpose is to allow
error.Error() to revert any changes made before the error occurred.

Entries in the Work Log should match against operations performed by
the OS Compatibility module in a 1:1 fasion.

OS Compatibility will save what it does to the Work Log, with any backup
file specified, so that they can be handled.

Work Log must also be able to clean up it's backup files every run.  The
backup files are either reverted to their original locations, or removed,
unless a backup is requested, in which case they are left for requested
functionality.
"""


# Our run-time state.  Module level storage.
#TODO(g): Any serious negatives to this?  I think its probably ok.
WORK_ITEMS = []


def AddItem(item):
  """Add a Work Log entry."""
  global WORK_ITEMS
  
  WORK_ITEMS.append(item)


def Revert():
  """Revert changes to backup or previous state, as there was a failure."""


def CleanUp():
  """Clean up backup files, as everything succeeded, and they are not needed
  to revert.
  """

