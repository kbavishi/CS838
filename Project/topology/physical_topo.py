"""An example of constructing a Hadoop profile with a NameNode PC, RM PC and `n'
slave PCs. The number of slaves are configurable

Instructions:
Wait for the profile instance to start, and then log in to either PC via the
ssh ports specified below.
"""

import geni.portal as portal
import geni.rspec.pg as rspec

pc = portal.Context()

pc.defineParameter("num_slaves", "Number of slave nodes",
                   portal.ParameterType.INTEGER, 3)
params = pc.bindParameters()

request = portal.context.makeRequestRSpec()

# Create a link with the type of LAN.
link = request.LAN("lan")

# Create the NameNode
nn_node = request.RawPC("namenode")
iface = nn_node.addInterface("if1")
link.addInterface(iface)

# Create the NameNode
rm_node = request.RawPC("resourcemanager")
iface = rm_node.addInterface("if2")
link.addInterface(iface)

# Create slave nodes.
for i in xrange(params.num_slaves):
    node = request.RawPC("slave%d" % i)
    iface = node.addInterface("if%d" % (i+3))
    link.addInterface(iface)

portal.context.printRequestRSpec()
