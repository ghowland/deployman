"""
Error Reporting
"""


from log import Log


class AbortCurrentDeployment(Exception):
  """Abort out of this current deployment, jumping back to where this is caught in the client.ProcessRequestsForever() function."""


def Error(text, options, exit_code=1):
  """Fail with an error. Options are required to deliver proper output."""
  Log('ERROR: %s\n' % text)
  
  # Abort this deployment
  raise AbortCurrentDeployment(text)

