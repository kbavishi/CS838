import os
import re
import shlex
import shutil
import signal
import sys
import time
import netaddr
import spur
from termcolor import cprint

USERNAME = "kbavishi"
# Original tarball used (not sure about ISA-L support)
#HADOOP_TAR_PATH = "http://apache.cs.utah.edu/hadoop/common/hadoop-3.0.0-alpha1/hadoop-3.0.0-alpha1.tar.gz"
# This is the tarball with greedy EC writes support (ISA-L support compiled in)
#HADOOP_TAR_PATH = "https://www.dropbox.com/s/1dc2ylj29j9cjv4/hadoop-3.0.0-alpha1.tar.gz"
# This is the tarball with both greedy EC reads and writes (ISA-L support
# compiled in)
HADOOP_TAR_PATH = "https://www.dropbox.com/s/o22gul7mwcx4k51/hadoop-3.0.0-alpha1.tar.gz"

# Issues that may still exist
# 1. Hosts file may be an issue

class TestShell(spur.SshShell):
    """
    Wrapper class around the spur.SshShell class. Provides the following
    benefits:

    1. Easier to run commands since it allows strings instead of just lists.
    Eg. It allows "ls -al", compared to spur.SshShell which only allows
    ["ls", "-al" ]

    2. Also handles the pipe (|) and redirect operators (>>) in a cleaner way
    than spur.SshShell

    3. Prints commands in colour. Easier to distinguish between the command
    being run and its output

    4. Also keeps track of spawned processes and cleans them on object deletion.
    [This doesn't quite work correctly yet]
    """
    def __init__(self, *args, **kwargs):
        self.processes = []
        super(TestShell, self).__init__(*args, **kwargs)

    def run_hadoop_cmd(self, cmd, *args, **kwargs):
        """Use this function to run any command defined in run.sh.
        Eg. start_all"""
        return self.run(["bash", "-c", "source run.sh; %s" % cmd],
                        *args, **kwargs)

    def run(self, cmd, *args, **kwargs):
        """Can run any bash command. Note:

        1. To run complex bash commands with pipes and redirectors, set
        "use_bash"=True in the kwargs just to be safe.
        Eg. shell.run("ps aux | grep DataNode", use_bash=True)

        2. Any commands defined in run.sh won't work. Use the function
        `run_hadoop_cmd` instead for those commands.
        """
        if isinstance(cmd, str):
            if ">>" in cmd or "|" in cmd or 'use_bash' in kwargs:
                # It is not possible to run commands which have pipes or the
                # redirect operators without this hack
                cmd = ["bash", "-c", cmd]
                kwargs.pop('use_bash', None)
            else:
                cmd = shlex.split(cmd)
        return super(TestShell, self).run(cmd, *args, **kwargs)

    def spawn(self, cmd, *args, **kwargs):
        if isinstance(cmd, str):
            # Convert the `str` type command to type `list`
            cmd = shlex.split(cmd)

        if 'stdout' not in kwargs:
            # Dump the output of the command to stdout. Without this, it is hard
            # to know if a command is stuck
            kwargs['stdout'] = sys.stdout

            # Print the command in yellow. This makes it easier to distinguish
            # from its output. Useful if a lot of output is dumped
            cprint(" ".join(cmd), 'yellow', attrs=['bold'])

        proc = super(TestShell, self).spawn(cmd, *args, **kwargs)
        self.processes += [proc]
        return proc

    def get_public_ip_addr(self):
        """Gives you the public IP address of the node"""
        result = self.run("hostname -I")
        ip_addrs = result.output.split(' ')
        for ip_addr in ip_addrs:
            if is_public_addr(ip_addr):
                return ip_addr
        assert False, "Could not find public IP address"

    def get_private_ip_addr(self, allow_public_ip=False):
        """Gives you the private IP address which will be used for HDFS
        communication. It assumes that they will be of the form 10.X.X.X"""

        result = self.run("hostname -I")
        ip_addrs = result.output.split(' ')
        private_ip_addrs = []
        # We know that CloudLab picks IP addrs of the form 10.x.x.x
        # Hack to take advantage of that
        for ip_addr in ip_addrs:
            if ip_addr.startswith('10.10'):
                private_ip_addrs += [ip_addr]

        if not private_ip_addrs:
            if allow_public_ip:
                # For GDA settings, we may be forced to use the public IP
                # address instead
                return self.get_public_ip_addr()
            else:
                assert False, "Could not find public IP address"
        else:
            # There can be multiple IP addresses belonging to different subnets
            # of the form 10.X.X.X. Pick the lexicographically first one
            return sorted(private_ip_addrs)[0]

    def __del__(self):
        """Tries to cleanup any processes that may be running"""
        for process in self.processes:
            try:
                process.send_signal(signal.SIGINT)
            except:
                # Process probably already dead
                continue

def is_public_addr(ip_addr):
    """Hacky utility function which guesses if given IP addr is a public IP addr
    or a private one"""
    return not ip_addr.startswith("10.") and not ip_addr.startswith("172.")

def copy_file(shell, local_path, remote_path):
    """Utility function to copy file present locally to the remote machine.
    Specify the path of the file to be copied over in `local_path`, and its
    destination path in `remote_path`"""

    with shell.open(remote_path, "wcb") as remote_file:
        with open(local_path, "rb") as local_file:
            shutil.copyfileobj(local_file, remote_file)

def create_ssh_shell(hostname, username=USERNAME, password=None, port=22):
    """Utility function which creates a TestShell class"""
    # Accept even if host key is missing. Without this, it just fails and quits.
    return TestShell(hostname=hostname, username=username,
                     password=password, port=port,
                     missing_host_key=spur.ssh.MissingHostKey.accept)

def install_dependencies(shell):
    """Installs all packages that will be needed for deploying Hadoop"""
    try:
        # Check if packages have already been installed to save time
        # We need Java 8 for running Hadoop 3.0.0-alpha1
        shell.run("dpkg -s openjdk-8-jdk")
        shell.run("dpkg -s pdsh")
    except:
        shell.run("sudo apt-get update --fix-missing")
        shell.run("sudo apt-get install -y software-properties-common")
        shell.run("sudo add-apt-repository -y ppa:openjdk-r/ppa")
        shell.run("sudo apt-get update")
        shell.run("sudo apt-get install -y openjdk-8-jdk")
        shell.run("sudo apt-get install -y pdsh")
        shell.run("sudo apt-get install -y stress")

def create_hadoop_dirs(shell):
    """Creates all logging and storage directories needed for running
    applications like HDFS, Spark etc. in Hadoop"""

    # Directories to be created
    dirs = ["conf", "logs", "software", "storage", "workload",
            "logs/apps", "logs/hadoop",
            "storage/data/local/nm", "storage/data/local/tmp",
            "storage/hdfs/hdfs_dn_dirs", "storage/hdfs/hdfs_nn_dir",
            "storage/data/spark/rdds_shuffle",
            "logs/spark", "storage/data/spark/worker"]

    shell.run("mkdir -p %s" % " ".join(dirs))

def setup_instances_file(shell, slave_ip_addrs):
    """Creates a file named `instances` containing all the slave IP addresses.
    This `instances` file is used by our `run.sh` script to start Hadoop daemons
    on the slaves"""
    # XXX /etc/hosts may not be okay

    # Delete the previous instances file. Without doing this, we risk adding
    # duplicate entries to the file
    shell.run("rm instances", allow_error=True)
    shell.run("touch instances")

    # Append all slave IP addrs to the file
    for slave_ip in slave_ip_addrs:
        shell.run("echo %s >> instances" % slave_ip)
    shell.run("cat instances")

def setup_intel_ISA_L(shell):
    try:
        shell.run("ls isa-l")
        # Probably already installed. Take a gamble and return
    except:
        shell.run("sudo apt-get -y install yasm")
        shell.run("git clone https://github.com/01org/isa-l.git")
        shell.run("cd isa-l; ./autogen.sh; ./configure; make; sudo make install",
                  use_bash=True)

def setup_conf_tar(shell, master_ip, link_awareness=False,
                   same_rack_penalty=5, parity_comp_cost=10,
                   disable_pipeline_sort='false'):
    """Copies over the XML conf files to the master"""

    # Create a tarball of conf files available locally and send it over.
    os.system("tar -cvzf conf.tar.gz conf/")
    copy_file(shell, "conf.tar.gz", "conf.tar.gz")

    # After sending it over, untar it and substitute with the master IP
    shell.run("tar -xvzf conf.tar.gz")
    for filename in shell.run("ls conf").output.split("\n"):
        if not filename:
            continue
        filepath = os.path.join("conf", filename)
        shell.run("sed -i -e 's/MASTER_IP/%s/g' %s" % (master_ip, filepath))

        # Also plug in values for same rack penalty and whether we want to
        # disable the pipeline sorting
        shell.run("sed -i -e 's/SAME_RACK_PENALTY/%s/g' %s" %
                  (same_rack_penalty, filepath))
        shell.run("sed -i -e 's/DISABLE_PIPELINE_SORT/%s/g' %s" %
                  (disable_pipeline_sort, filepath))
        shell.run("sed -i -e 's/PARITY_COMP_COST/%s/g' %s" %
                  (parity_comp_cost, filepath))

        # Enable link awareness if it was requested.
        if link_awareness:
            shell.run("sed -i -e "\
            "'s/LINK_AWARENESS/\/users\/kbavishi\/link_awareness.py/g' %s" %
                      filepath)

def setup_run_sh(shell):
    """Copies over the run.sh script needed for running Hadoop daemons"""
    copy_file(shell, "run.sh", "run.sh")
    # NOTE: We need to update the SPARK_MASTER_IP if we intend to use Spark

def copy_native_libraries(shell):
    cmd = "sudo cp software/hadoop-3.0.0-alpha1/lib/native/* /usr/lib/"
    try:
        output = shell.run(cmd, use_bash=True)
    except spur.RunProcessError, e:
        if "omitting directory" in e.message:
            # This is a known exception
            pass
        else:
            raise e

def setup_hadoop_tar(shell, master_shell=None, allow_public_ip=False):
    """Downloads the Hadoop tarball and extracts it.
    If this is a slave node and a master shell has been provided, we will try to
    scp it over instead of downloading it"""
    try:
        # The tarball is huge. Don't download if already present
        shell.run("ls hadoop-3.0.0-alpha1.tar.gz")
    except:
        # Download the tarball since it doesn't exist
        ip_addr = shell.get_private_ip_addr(allow_public_ip=allow_public_ip)

        if master_shell and ip_addr.startswith("10.10"):
            # It will be faster to copy the tarball over from the master instead
            # of downloading it. This will work only if passwordless login has
            # been setup and if it is a non-GDA slave.
            scp_cmd = "scp hadoop-3.0.0-alpha1.tar.gz %s:~/" % ip_addr
            master_shell.run(scp_cmd)
        else:
            shell.run("wget %s" % HADOOP_TAR_PATH)
        shell.run("tar -xzf hadoop-3.0.0-alpha1.tar.gz -C software")

        # This needs to be done for some reason because it can never find native
        # libraries
        copy_native_libraries(shell)

def kill_old_instances(shell):
    """Kill any previously running Hadoop daemons"""
    # There are previously running instances of DataNode and NameNode if you
    # choose the Hadoop topology in CloudLab. Although we can use them directly,
    # our approach is more customizable. So kill those instances
    try:
        shell.run_hadoop_cmd("stop all", allow_error=True)
        shell.run("pgrep -f '(NameNode|DataNode|NodeManager)' | xargs sudo kill")
    except:
        pass

def setup_cpu_governor(shell, governor):
    """Set a specific CPU scaling governor.
    The recognized governors are: conservative, ondemand, userspace, powersave,
    performance.
    """
    # First check if the necessary packages have been installed
    pkgs = ["linux-tools-common", "linux-tools-3.13.0-100-generic",
            "linux-cloud-tools-3.13.0-100-generic"]
    try:
        for pkg in pkgs:
            shell.run("dpkg -s %s > /dev/null" % pkg, use_bash=True)
    except:
        shell.run("sudo apt-get -y install %s" % " ".join(pkgs))

    shell.run("sudo cpupower frequency-set -g %s" % governor)

def setup_hadoop(shell, master_ip, master_shell=None,
                 allow_public_ip=False, link_awareness=False,
                 same_rack_penalty=5, parity_comp_cost=10,
                 disable_pipeline_sort='false',
                 gd_rack=None):
    """Sets up everything that is needed to run Hadoop on the cluster"""

    # Kill any previously running daemons
    kill_old_instances(shell)

    # Install all packages that will be needed. If they are already installed,
    # the function will return quietly
    install_dependencies(shell)

    # Create all directories needed for storing logs, container outputs etc.
    create_hadoop_dirs(shell)

    # Copy over the XML configs
    shell_ip = shell.get_private_ip_addr(allow_public_ip=allow_public_ip)
    if is_public_addr(shell_ip):
        # This seems to be a node using its public IP addr ie. it must be in a
        # different GD cluster. Use the public IP address of the master instead
        # of its private one
        assert master_shell, "Master shell should have been provided"
        master_ip = master_shell.get_public_ip_addr()

    if link_awareness:
        # Copy over link awareness script
        setup_link_awareness(shell, gd_rack)

    setup_conf_tar(shell, master_ip, link_awareness, same_rack_penalty,
                   parity_comp_cost, disable_pipeline_sort)

    # Copy over the run.sh script for running daemons
    setup_run_sh(shell)

    # Copy over rack awareness script 
    setup_rack_awareness(shell)

    # Setup Intel ISA-L libraries
    setup_intel_ISA_L(shell)

    # Download the Hadoop tarball if it doesn't exist
    setup_hadoop_tar(shell, master_shell=master_shell,
                     allow_public_ip=allow_public_ip)

    # Setup high performance governor. This is extremely important to get
    # uniform results. Otherwise we may observe strange results like EC with
    # parity computation getting better throughput than EC with zero computation.
    setup_cpu_governor(shell, "performance")

def setup_passwordless(nn_shell, slave_shells, allow_public_ip=False):
    """Sets up passwordless access between master and slave nodes. Needed for
    running Hadoop daemons"""
    try:
        # Check if the RSA public key already exists
        nn_shell.run("ls .ssh/id_rsa.pub")
    except:
        # Need to create an RSA key since nothing exists
        nn_shell.run("ssh-keygen -f /users/kbavishi/.ssh/id_rsa -t rsa -N '' ",
                     use_bash=True)

    # Add the public RSA key to the authorized_keys list on each slave
    publickey = nn_shell.run("cat .ssh/id_rsa.pub").output.strip("\n").strip()
    for slave_shell in slave_shells:
        try:
            # Check if it has already been added
            slave_shell.run("grep '%s' .ssh/authorized_keys" % publickey,
                            use_bash=True)
        except:
            # Hasn't been added. Update the authorized_keys list
            slave_shell.run("echo -e '%s' >> .ssh/authorized_keys" % publickey)
            ip_addr = \
                slave_shell.get_private_ip_addr(allow_public_ip=allow_public_ip)
            # Add the slave's IP address to the known_hosts list on the master.
            # Creates issues otherwise
            nn_shell.run("ssh-keyscan -H %s >> .ssh/known_hosts" % ip_addr)

def setup_rack_awareness(shell):
    """Copies over script which will provide rack awareness to HDFS."""
    # Need to install dependencies: pip and netaddr
    try:
        shell.run("dpkg -s python-pip")
        shell.run("pip list | grep pip")
    except:
        cmds = ["sudo apt-get install -y python-pip",
                "sudo pip install netaddr"]
        for cmd in cmds:
            shell.run(cmd)

    copy_file(shell, "rack_awareness.py", "rack_awareness.py")
    shell.run("chmod +x rack_awareness.py")

def setup_link_awareness(shell, gd_rack):
    """Copies over script which will bring link cost awareness to HDFS."""
    copy_file(shell, "link_awareness.py", "link_awareness.py")
    shell.run("chmod +x link_awareness.py")
    # The only way I could think of to not hardcode GD rackname in the script
    shell.run("sed -i -e 's/GD_RACK/\%s/g' link_awareness.py" % gd_rack)

def format_namenode(shell):
    """Formats HDFS namenode. I believe this is for setting the namespace ID on
    the NameNode which is then used in the handshakes between DataNodes and
    NameNodes"""
    try:
        # Formatting should be done only once. So check for the presence of the
        # file `formatting_done`.
        shell.run("ls formatting_done")
    except:
        shell.run_hadoop_cmd("hadoop namenode -format")
        # Create indicator file. Its presence will be used to ensure that we
        # don't end up formatting again.
        shell.run("touch formatting_done")

def get_net_stats(shell):
    """
    Find network stats for a given node.
    """
    # face |bytes    packets errs drop fifo frame compressed multicast|bytes
    # packets errs drop fifo colls carrier compressed
    output = shell.run("cat /proc/net/dev").output
    ip_addr = shell.get_private_ip_addr(allow_public_ip=True)
    intf_name = shell.run("netstat -ie | grep -B1 '%s' | head -n1 | "
                          "awk '{print $1}'" % ip_addr).output.strip()

    for line in output.split("\n"):
        line = line.strip()
        if line.startswith(intf_name):
            stats = line.split()
            # Fetch rxBytes, txBytes
            return int(stats[1]), int(stats[9])

    return None

def get_wan_netstats(slave_shells):
    """
    Appends WAN usage for slave nodes that are running on another GD cluster.
    """
    wan_output = {}
    for slave_shell in slave_shells:
        ip_addr = slave_shell.get_private_ip_addr(allow_public_ip=True)
        if not is_public_addr(ip_addr):
            # Don't need netstats for nodes on the same cluster
            continue
        wan_output[ip_addr] = get_net_stats(slave_shell)

    return wan_output

def drop_caches(shells):
    for shell in shells:
        shell.run("echo 3 | sudo tee /proc/sys/vm/drop_caches", use_bash=True)

def run_stress_procs(shells, load=0.5):
    # Kill any previously running stress procs
    kill_stress_procs(shells)

    for shell in shells:
        cmd = "cat /proc/cpuinfo | grep processor | wc -l"
        cores = int(shell.run(cmd).output)
        shell.spawn("stress --cpu %d" % int(load*cores))

def kill_stress_procs(shells):
    for shell in shells:
        shell.run("sudo pkill stress", allow_error=True)

def verify_ec_policy(shell, path, dataBlkNum, parityBlkNum):
    blockLine = "BP-\S* len=\d+ Live_repl=(\d+)"
    output = shell.run_hadoop_cmd("hdfs fsck %s -files -blocks" % path).output
    replicas = re.findall(blockLine, output)
    print replicas
    assert all(map(lambda r: int(r) == (dataBlkNum + parityBlkNum), replicas)), \
        "Some blocks do not have the necessary number of replicas"

def run_TestDFSIO(shell, slave_shells, result_file="results.out",
                  test_type="write", number_of_files=1, file_size='1MB'):
    """
    Run TestDFSIO with various options. Assumes that start_all has been run
    before. Check out the TestDFSIO documentation for more info about the test
    parameters.
    """
    dfsio_jar = ("software/hadoop-3.0.0-alpha1/share/hadoop/mapreduce/"
                 "hadoop-mapreduce-client-jobclient-3.0.0-alpha1-tests.jar")
    cmd = "yarn jar %s TestDFSIO" % dfsio_jar
    cmd += " -resFile %s" % result_file
    cmd += " -%s" % test_type
    cmd += " -nrFiles %s" % number_of_files
    cmd += " -size %s" % file_size

    # Drop VM caches. This is extremely important to rule out any effects due to
    # caching
    drop_caches([shell] + slave_shells)

    wan_usage_before = get_wan_netstats(slave_shells)

    usage_before = {}
    usage_after = {}
    for slave_shell in [shell] + slave_shells:
        usage_before[slave_shell.get_private_ip_addr(allow_public_ip=False)] = \
            get_net_stats(slave_shell)

    output = shell.run_hadoop_cmd(cmd).output

    wan_usage_after = get_wan_netstats(slave_shells)

    for slave_shell in [shell] + slave_shells:
        usage_after[slave_shell.get_private_ip_addr(allow_public_ip=False)] = \
            get_net_stats(slave_shell)

    for ip_addr in usage_before:
        shell_usage_before = usage_before[ip_addr]
        shell_usage_after = usage_after[ip_addr]
        output += "USAGE %s RX: %d bytes\n" % (ip_addr,
                                               shell_usage_after[0] - 
                                               shell_usage_before[0])
        output += "USAGE %s TX: %d bytes\n" % (ip_addr,
                                               shell_usage_after[1] -
                                               shell_usage_before[1])


    # Append WAN usage before for slaves running in another GD cluster
    if wan_usage_before:
        for ip_addr in wan_usage_before:
            rxb_before, txb_before = wan_usage_before[ip_addr]
            rxb_after, txb_after = wan_usage_after[ip_addr]
            output += "WAN RX for %s: %d bytes\n" % (ip_addr,
                                                     rxb_after - rxb_before)
            output += "WAN TX for %s: %d bytes\n" % (ip_addr,
                                                     txb_after - txb_before)
            print "WAN Usage for %s: %s, %s\n" % (ip_addr,
                                                  rxb_after - rxb_before,
                                                  txb_after - txb_before)

    # Check that DataNode processes have not crashed because of the test
    check_datanode_health(shell)

    return output

def cleanup_TestDFSIO(shell):
    """Cleans up the /benchmarks directory on HDFS so that subsequent testcases
    can run safely."""
    shell.run_hadoop_cmd("hadoop fs -rm -f -r /benchmarks")

    # Wait for it to actually disappear. Otherwise if we shut down HDFS daemons
    # immediately, it reappears
    while True:
        try:
            shell.run_hadoop_cmd("hadoop fs -ls /benchmarks")
        except:
            # We hit an exception which it means it finally has been deleted
            break

def save_output(output, filename):
    """Saves the string output to a file in the `output` directory"""
    path = os.path.join("output", filename)
    open(path, "w").write(output)

def parse_host(host_str):
    """Parses strings of the format <hostname:port> and returns the necessary
    fqdn and port"""
    values = host_str.split(':')
    if len(values) == 1:
        # If port hasn't been provided, assume the default SSH port 22
        host, port = values[0], 22
    elif len(values) == 2:
        host, port = values
    else:
        assert False, "Unparseable string: %s" % host_str

    # If domain name is not provided, assume it is a node on Wisc CloudLab
    if "cloudlab" not in host:
        domain_name = ".wisc.cloudlab.us"
        host += domain_name

    return host, port

def setup_hadoop_testbase(namenode, resourcemgr, slaves,
                          allow_public_ip=False, link_awareness=False,
                          same_rack_penalty=5, parity_comp_cost=10,
                          disable_pipeline_sort='false'):
    """Sets up everything needed for Hadoop to run on the cluster.
    Parameters:

    @namenode: String of the form <hostname:port> containing NN info

    @resourcemgr: String of the form <hostname:port> containing RM info

    @slaves: List of string of the form <hostname:port> containing slave info

    @allow_public_ip: Set to True if you are running Hadoop on a GDA setting

    @link_awareness: Set to True if you want the link awareness script to be
    installed.

    @same_rack_penalty: Set to an integer to set a custom same rack penalty.
    Useful if you want it to deliberately ignore GDA greedy heuristics.

    @parity_comp_cost: Set to an integer to set a custom parity computation
    cost.

    @disable_pipeline_sort: Set to False if you want to disable the pipeline
    sort algorithm while selecting datanodes. This is very useful if you want to
    avoid selecting distant nodes just for parity chunks and not data chunks.

    Returns the master shell for running testcases"""

    # Assume all entries are VMs and have ports embedded
    # Create TestShell for the master
    nn_hostname, nn_port = parse_host(namenode)
    nn_shell = create_ssh_shell(nn_hostname, port=int(nn_port))
    master_ip = nn_shell.get_private_ip_addr()

    # Create TestShell for the resource manager
    rm_hostname, rm_port = parse_host(namenode)
    rm_shell = create_ssh_shell(rm_hostname, port=int(rm_port))

    # NOTE: We are not running a master and slave on the same instance
    slave_ip_addrs = []
    slave_shells = []

    # Create TestShells for the slaves
    assert isinstance(slaves, list)
    for slave in slaves:
        slave_hostname, slave_port = parse_host(slave)
        slave_shell = create_ssh_shell(slave_hostname, port=int(slave_port))
        # We may want to allow public IP addrs for slaves if it is GDA setting
        slave_ip_addrs += \
            [slave_shell.get_private_ip_addr(allow_public_ip=allow_public_ip)]
        slave_shells += [slave_shell]

    # Setup passwordless access between the master and each of the slave nodes
    setup_passwordless(nn_shell, slave_shells, allow_public_ip=allow_public_ip)

    # Setup instances file with slave IP addrs. This will be used by our scripts
    # for running daemons on slave nodes
    setup_instances_file(nn_shell, slave_ip_addrs)

    gd_rack = None
    if link_awareness:
        # Need to know the rack name for the node in another DC.
        for ip in slave_ip_addrs:
            if ip.startswith("10.10"):
                continue
            netmask = '255.255.255.0'
            address = '{0}/{1}'.format(ip, netmask)
            network_address = netaddr.IPNetwork(address).network
            gd_rack = '/{0}'.format(network_address)

        assert gd_rack, "Could not find rackname of node in another DC"

    # Setup everything needed for running Hadoop on the cluster
    # XXX Ignore RM for now
    for shell in [nn_shell,]:
        setup_hadoop(shell, master_ip, link_awareness=link_awareness,
                     same_rack_penalty=same_rack_penalty,
                     parity_comp_cost=parity_comp_cost,
                     disable_pipeline_sort=disable_pipeline_sort,
                     gd_rack=gd_rack)

    # Setup for slaves is a little different because they can use the master for
    # scp'ing over certain tarballs
    for shell in slave_shells:
        # XXX: Could be done in parallel
        setup_hadoop(shell, master_ip, master_shell=nn_shell,
                     allow_public_ip=allow_public_ip,
                     link_awareness=link_awareness,
                     same_rack_penalty=same_rack_penalty,
                     parity_comp_cost=parity_comp_cost,
                     disable_pipeline_sort=disable_pipeline_sort,
                     gd_rack=gd_rack)

    # Format the NameNode. This is needed only once
    format_namenode(nn_shell)

    # Ensure that no previously running daemons exist. Note that before any test
    # is run, "start all" has to be called
    stop_all(nn_shell, allow_error=True)

    return nn_shell, rm_shell, slave_shells

def set_ec_policy(shell, path, policy):
    """Sets an erasure code policy for the given path. Allowed values for EC
    policy are:
    1. RS-DEFAULT-3-2-64k
    2. RS-DEFAULT-6-3-64k
    3. RS-LEGACY-3-2-64k
    """
    # Check if EC policy has already been set. We get an error otherwise
    if policy in get_ec_policy(shell, path):
        return

    ec_cmd = "hdfs erasurecode -setPolicy -p %s %s" % (policy, path)
    shell.run_hadoop_cmd(ec_cmd)

def get_ec_policy(shell, path):
    """Gets the current erasure code policy for the given path."""
    ec_cmd = "hdfs erasurecode -getPolicy %s" % path
    return shell.run_hadoop_cmd(ec_cmd).output

def check_datanode_health(shell, wait_time=None):
    """Checks if the DataNode processes are running on the slave nodes."""
    if wait_time:
        time.sleep(wait_time)

    dn_topo_output = shell.run_hadoop_cmd("hadoop dfsadmin -printTopology").output
    slave_ip_addrs = shell.run("cat instances").output.strip().split("\n")
    for ip_addr in slave_ip_addrs:
        if not ip_addr:
            continue
        # Check whether the DataNode process is running on the slave
        try:
            shell.run("ssh %s pgrep -f DataNode" % ip_addr)
        except:
            assert False, "DataNode not running on %s" % ip_addr

        # Check whether the NameNode is receiving heartbeats from the DataNode
        assert ip_addr in dn_topo_output, \
               "NN is not receiving heartbeats from DataNode %s. "\
               "Please check datanode logs."

def set_slaves_hostnames(slave_shells):
    """Sets the expected hostnames for slaves in the same cluster. This is
    somehow needed for our GDA tests."""
    for slave_shell in slave_shells:
        ip_addr = slave_shell.get_private_ip_addr(allow_public_ip=True)
        if is_public_addr(ip_addr):
            continue

        # Fix hostname
        hostname = slave_shell.run("hostname").output.strip()
        # Drop the domain name
        hostname = hostname.split(".")[0]
        slave_shell.run("sudo hostname %s" % hostname)

def start_all(shell):
    """Starts all HDFS daemons using the run.sh script on the namenode. Also
    verifies that everything started as expected. We sometimes see arbitrary
    errors."""

    shell.run_hadoop_cmd("start_all")
    # Check that DataNode was started correctly on all the slave nodes
    check_datanode_health(shell, wait_time=5)

def stop_all(shell, allow_error=False):
    """Stops all HDFS daemons using the run.sh script on the namenode."""
    shell.run_hadoop_cmd("stop_all", allow_error=allow_error)
