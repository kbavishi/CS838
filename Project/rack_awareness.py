#!/usr/bin/python

# This script makes assumptions about the physical environment.
#  1) each rack is its own layer 3 network with a /24 subnet, which
# could be typical where each rack has its own
#     switch with uplinks to a central core router.
#
#             +-----------+
#             |core router|
#             +-----------+
#            /             \
#   +-----------+        +-----------+
#   |rack switch|        |rack switch|
#   +-----------+        +-----------+
#   | data node |        | data node |
#   +-----------+        +-----------+
#   | data node |        | data node |
#   +-----------+        +-----------+
#
# 2) topology script gets list of IP's as input, calculates network address, and prints '/network_address/ip'.

import netaddr
import sys

# Discard name of topology script from argv list as we just want IP addresses
sys.argv.pop(0)

# Set netmask to what's being used in your environment.  The example uses a /24
netmask = '255.255.255.0'

# Loop over list of datanode IPs
for ip in sys.argv:
    # format address string so it looks like 'ip/netmask' to make netaddr work
    address = '{0}/{1}'.format(ip, netmask)
    try:
        # calculate and print network address
        network_address = netaddr.IPNetwork(address).network
        print "/{0}".format(network_address)
    except:
        # Print catch-all value if unable to calculate network address
        print "/rack-unknown"
