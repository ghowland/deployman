"""
RunMan Client: Run forever processing server requests
"""


import yaml
import urllib2
import urllib
import base64
import json
import hashlib
import time
import signal
import sys
import logging

from log import log
from error import AbortCurrentDeployment

import run
import platform

import install


# Loop for second before checking again
LOOP_DELAY = 10.0

# Default to running, SIGKILL changes that
RUNNING = True

# Maximum errors in a row before we quit, so we dont freak out
MAX_CONSECUTIVE_ERRORS = 5

# Seconds to wait after a consecutive processing error (more than 1 error in a row)
CONSECUTIVE_ERROR_WAIT = 5.0


def SignalHandler_Quit(signum, frame):
  """Quit during a safe time after receiving a quit signal."""
  log('Received signal to quit: %s' % signum)
  
  global RUNNING
  RUNNING = False

  
def ProcessRequestsForever(config, command_options, command_args):
  global RUNNING
  
  log('Starting')
  log('Running forever in Client Mode... (%s) [%s]' % (platform.GetHostname(), platform.GetPlatform()))


  # Ensure that if we get a SIGTERM signal that we quit properly
  signal.signal(signal.SIGTERM, SignalHandler_Quit)


  # Create data to pass to the web request
  job_get_data = {'hostname':platform.GetHostname()}
  
  consecutive_errors = 0
  
  # Run forever, until we quit
  while RUNNING:
    #try:
    if 1:
      # Get the deployments the server has for us
      log('Getting deployments')
      result = WebGet(config['deploy_source'], job_get_data)
      server_result = json.loads(result)
      
      # If we got an error when requesting deployments, show it
      if 'error' in server_result:
        log('Deploy Source error: %s' % server_result['error'], logging.ERROR)
        
        # Try after a break
        time.sleep(CONSECUTIVE_ERROR_WAIT)
        continue
      
      log('Server Result Deployments: %s (%s)' % (server_result['deployments'], type(server_result['deployments'])))
      deployments = json.loads(server_result['deployments'])
      
      # Loop over the deployments the server gave us
      for deployment in deployments:
        log('Processing deployment request: %s: %s: %s' % (deployment['id'], deployment['component'], deployment['component_instance']))
        
        # Sysync installation initiatiation
        result = install.InstallSystem(config, deployment, command_options)
        log('Install Result: %s' % result)
        
        result_json = json.dumps(result, sort_keys=True)
        
        # Report the results
        try:
          report_result = WebGet(config['deploy_report'], {'id':deployment['id'], 'data':result_json})
          log('Report Result: %s' % report_result)
        except Exception, e:
          log('Failed to report results: %s' % e)
        
      
      # Sleep - Give back to the system, if we are going to keep running (otherwise, quit faster)
      #log('Sleeping... (%s seconds)' % LOOP_DELAY)
      if RUNNING:
        log('Finished.  Sleeping...')
        time.sleep(LOOP_DELAY)
        
        # Clear any consecutive errors, we made it to the end of processing
        consecutive_errors = 0
    
    ## Handle errors, where we abort this deployment
    #except AbortCurrentDeployment, e:
    #  # Report the results
    #  try:
    #    report_result = WebGet(config['job_report'], {'id':deployment['id'], 'data':'''{"errors":["%s"]}''' % e.message})
    #    log('Report Result: %s' % report_result)
    #  except Exception, e2:
    #    log('Failed to report error results: %s:  Original Error: %s' % (e2, e))
    #
    ## General errors
    #except Exception, e:
    #  log('Main loop exception:\n\n%s' % e)
    #  consecutive_errors += 1
    #  
    #  # Reaches max?  Quit
    #  if consecutive_errors > MAX_CONSECUTIVE_ERRORS:
    #    log('Consecutive errors about max (%s), sleeping...' % MAX_CONSECUTIVE_ERRORS)
    #    time.sleep(CONSECUTIVE_ERROR_WAIT)
    #  
    #  # Else, if we have had 2 errors in a row, sleep for a back-off time
    #  elif consecutive_errors >= 2:
    #    log('Sleeping for consecutive error backoff: %s seconds' % CONSECUTIVE_ERROR_WAIT)
    #    time.sleep(CONSECUTIVE_ERROR_WAIT)
    

def WebGet(config, args=None):
  """Wrap dealing with web requests.  The job server uses this to avoid giving out database credentials to all machines."""
  #log('WebGet: %s' % config)
  try:
    http_request = urllib2.Request(config['url'])
  
    # If Authorization
    if config.get('username', None):
      auth = base64.standard_b64encode('%s:%s' % (config['username'], config['password'])).replace('\n', '')
      http_request.add_header("Authorization", "Basic %s" % auth)
    
    
    # If args (POST)
    if args:
      http_request.add_data(urllib.urlencode(args))
  
  
    result = urllib2.urlopen(http_request)
    data = result.read()
    
    return data

  except Exception, e:
    log('WebGet error: %s: %s: %s' % (e, config, args))
    
    # No deployments, just keep going, we logged the error (in JSON format)
    return """{"deployments":"[]"}"""


