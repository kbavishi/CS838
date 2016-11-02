import geni.portal as portal
import geni.rspec.pg as RSpec
import geni.rspec.igext

pc = portal.Context()

pc.defineParameter( "n", "Number of slave nodes",
		    portal.ParameterType.INTEGER, 3 )

pc.defineParameter( "raw", "Use physical nodes",
                    portal.ParameterType.BOOLEAN, False )

pc.defineParameter( "mem", "Memory per VM",
		    portal.ParameterType.INTEGER, 256 )

params = pc.bindParameters()

#IMAGE = "https://www.apt.emulab.net/image_metadata.php?uuid=b60fe4ec-5d64-11e5-9efe-45d11788de58"
IMAGE = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU16-64-STD"
SETUP = "http://www.emulab.net/downloads/hadoop-2.7.1-setup.tar.gz"

class PublicVM(geni.rspec.igext.XenVM):
  def __init__ (self, name, component_id = None, exclusive = False):
    super(PublicVM, self).__init__(name)

  def _write (self, root):
    nd = super(PublicVM, self)._write(root)
    nd.attrib["{http://www.protogeni.net/resources/rspec/ext/emulab/1}routable_control_ip"] = str( "" )
    return nd

def Node( name, public ):
    if params.raw:
        return RSpec.RawPC( name )
    elif public:
        vm = PublicVM( name )
        vm.ram = params.mem
        return vm
    else:
        vm = geni.rspec.igext.XenVM( name )
        vm.ram = params.mem
        return vm

rspec = RSpec.Request()

lan = RSpec.LAN()
rspec.addResource( lan )

node = Node( "namenode", True )
node.disk_image = IMAGE
iface = node.addInterface( "if0" )
lan.addInterface( iface )
rspec.addResource( node )

node = Node( "resourcemanager", True )
node.disk_image = IMAGE
iface = node.addInterface( "if0" )
lan.addInterface( iface )
rspec.addResource( node )

for i in range( params.n ):
    node = Node( "slave" + str( i ), False )
    node.disk_image = IMAGE
    iface = node.addInterface( "if0" )
    lan.addInterface( iface )
    rspec.addResource( node )

from lxml import etree as ET

tour = geni.rspec.igext.Tour()
tour.Description( geni.rspec.igext.Tour.TEXT, "A cluster for running Hadoop. It includes a name node, a resource manager, and as many slaves as you choose." )
tour.Instructions( geni.rspec.igext.Tour.MARKDOWN, "Nothing has been installed.  Have fun!" )
rspec.addTour( tour )

pc.printRequestRSpec( rspec )
