"""An example of constructing a Hadoop profile with a NameNode PC, RM PC and n
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

longDesc = "A specific hardware type to use for each node.  Cloudlab clusters "
"all have machines of specific types.  When you set this field to a value that "
"is a specific hardware type, you will only be able to instantiate this profile "
"on clusters with machines of that type.  If unset, when you instantiate the "
"profile, the resulting experiment may have machines of any available type "
"allocated."

pc.defineParameter("osNodeType", "Hardware type of all nodes",
                   portal.ParameterType.NODETYPE, "",
                   longDescription=longDesc)

params = pc.bindParameters()

request = portal.context.makeRequestRSpec()

# Create a link with the type of LAN.
link = request.LAN("lan")

# Create the NameNode
nn_node = request.RawPC("namenode")
if params.osNodeType:
    nn_node.hardware_type = params.osNodeType
iface = nn_node.addInterface("if1")
link.addInterface(iface)

# Create the NameNode
rm_node = request.RawPC("resourcemanager")
if params.osNodeType:
    rm_node.hardware_type = params.osNodeType
iface = rm_node.addInterface("if2")
link.addInterface(iface)

# Create slave nodes.
for i in xrange(params.num_slaves):
    node = request.RawPC("slave%d" % i)
    if params.osNodeType:
        node.hardware_type = params.osNodeType
    iface = node.addInterface("if%d" % (i+3))
    link.addInterface(iface)

portal.context.printRequestRSpec()
