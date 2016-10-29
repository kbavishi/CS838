import os
import shlex
import shutil
import signal
import sys
import spur
from termcolor import cprint

USERNAME = "kbavishi"
#HADOOP_TAR_PATH = "https://archive.apache.org/dist/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz"
HADOOP_TAR_PATH = "http://apache.cs.utah.edu/hadoop/common/hadoop-3.0.0-alpha1/hadoop-3.0.0-alpha1.tar.gz"

# Issues I think I have resolved
# 1. Passwordless login - Asks you for yes/no prompt
# 2. Hosts file may be an issue
# 3. There seems to be some NameNode process running on the topology at the
# beginning

class TestShell(spur.SshShell):
    """
    Wrapper class around the SshShell class. Provides easier way to run
    commands by accepting strings.

    Also keeps track of spawned processes and cleans them on object deletion.
    This makes writing tests easier because you don't need cleanup code in each
    test
    """
    def __init__(self, *args, **kwargs):
        self.processes = []
        super(TestShell, self).__init__(*args, **kwargs)

    def run_hadoop_cmd(self, cmd, *args, **kwargs):
        return self.run(["bash", "-c", "source run.sh; %s" % cmd],
                        *args, **kwargs)

    def run(self, cmd, *args, **kwargs):
        if type(cmd) == str:
            if ">>" in cmd or "|" in cmd or 'use_bash' in kwargs:
                # It is not possible to run commands which have pipes or the
                # redirect operators without this hack
                cmd = ["bash", "-c", cmd]
                kwargs.pop('use_bash', None)
            else:
                cmd = shlex.split(cmd)
        return super(TestShell, self).run(cmd, *args, **kwargs)

    def spawn(self, cmd, *args, **kwargs):
        if type(cmd) == str:
            cmd = shlex.split(cmd)
        if 'stdout' not in kwargs:
            kwargs['stdout'] = sys.stdout
            cprint(" ".join(cmd), 'yellow', attrs=['bold'])
        proc = super(TestShell, self).spawn(cmd, *args, **kwargs)
        self.processes += [proc]
        return proc

    def get_ip_addr(self):
        result = self.run("hostname -I")
        ip_addrs = result.output.split(' ')
        # We know that CloudLab picks IP addrs of the form 10.x.x.x
        # Hack to take advantage of that
        for ip_addr in ip_addrs:
            if ip_addr.startswith('10.'):
                return ip_addr
        assert False, "Could not find IP address"

    def __del__(self):
        print 'HERE!!'
        for process in self.processes:
            try:
                process.send_signal(signal.SIGINT)
                print 'KILLED proc SUCCESSFULLY'
            except:
                # Process probably already dead
                print 'FAILED to KILL proc'
                continue

def copyFile(shell, localPath, remotePath):
    with shell.open(remotePath, "wcb") as remote_file:
        with open(localPath, "rb") as local_file:
            shutil.copyfileobj(local_file, remote_file)

def create_ssh_shell(hostname, username=USERNAME, password=None, port=22):
    # Accept even if host key is missing
    return TestShell(hostname=hostname, username=username,
                     password=password, port=port,
                     missing_host_key=spur.ssh.MissingHostKey.accept)

def install_dependencies(shell):
    try:
        # Check if packages have already been installed to save time
        shell.run("dpkg -s openjdk-8-jdk")
        shell.run("dpkg -s pdsh")
    except:
        shell.run("sudo apt-get update --fix-missing")
        shell.run("sudo apt-get install -y software-properties-common")
        shell.run("sudo add-apt-repository -y ppa:openjdk-r/ppa")
        shell.run("sudo apt-get update")
        shell.run("sudo apt-get install -y openjdk-8-jdk")
        shell.run("sudo apt-get install -y pdsh")

def create_hadoop_dirs(shell):
    dirs = ["conf", "logs", "software", "storage", "workload",
            "logs/apps", "logs/hadoop",
            "storage/data/local/nm", "storage/data/local/tmp",
            "storage/hdfs/hdfs_dn_dirs", "storage/hdfs/hdfs_nn_dir",
            "storage/data/spark/rdds_shuffle",
            "logs/spark", "storage/data/spark/worker"]

    shell.run("mkdir -p %s" % " ".join(dirs))

def setup_instances_file(shell, slave_ip_addrs):
    # XXX /etc/hosts may not be okay
    shell.run("rm instances", allow_error=True)
    shell.run("touch instances")
    for slave_ip in slave_ip_addrs:
        shell.run("echo %s >> instances" % slave_ip)
    shell.run("cat instances")

def setup_conf_tar(shell, master_ip):
    # Create a tarball of conf files and send it over.
    # After sending it over, substitute with the master IP
    os.system("tar -cvzf conf.tar.gz conf/")
    copyFile(shell, "conf.tar.gz", "conf.tar.gz")

    shell.run("tar -xvzf conf.tar.gz")
    for filename in shell.run("ls conf").output.split("\n"):
        if not filename:
            continue
        filepath = os.path.join("conf", filename)
        shell.run("sed -i -e 's/MASTER_IP/%s/g' %s" % (master_ip, filepath))

def setup_run_sh(shell):
    # Deploy run.sh
    copyFile(shell, "run.sh", "run.sh")
    # NOTE: We need to update the SPARK_MASTER_IP if we intend to use Spark

def setup_hadoop_tar(shell):
    # We need the Hadoop tarball
    try:
        # The tarball is huge. Don't download if already present
        shell.run("ls hadoop-3.0.0-alpha1.tar.gz")
    except:
        # Download the tarball
        shell.run("wget %s" % HADOOP_TAR_PATH)
        shell.run("tar -xzf hadoop-3.0.0-alpha1.tar.gz -C software")

def kill_old_instances(shell):
    # There are previously running instances of DataNode and NameNode if you
    # choose the Hadoop topology in CloudLab. Although we can use them directly,
    # our approach is more customizable. So kill those instances
    try:
        shell.run_hadoop_cmd("stop all", allow_error=True)
        shell.run("pgrep -f '(NameNode|DataNode|NodeManager)' | xargs sudo kill")
    except:
        pass

def setup_hadoop(shell, master_ip, slave_ip_addrs):
    kill_old_instances(shell)
    install_dependencies(shell)
    create_hadoop_dirs(shell)
    setup_instances_file(shell, slave_ip_addrs)
    setup_conf_tar(shell, master_ip)
    setup_run_sh(shell)
    setup_hadoop_tar(shell)

def setup_passwordless(nn_shell, slave_shells):
    try:
        # Check if the RSA public key already exists
        nn_shell.run("ls .ssh/id_rsa.pub")
    except:
        # Need to create one since nothing exists
        nn_shell.run("ssh-keygen -f /users/kbavishi/.ssh/id_rsa -t rsa -N '' ",
                     use_bash=True)

    publickey = nn_shell.run("cat .ssh/id_rsa.pub").output.strip("\n").strip()
    for slave_shell in slave_shells:
        try:
            # Check if it has already been added
            slave_shell.run("grep '%s' .ssh/authorized_keys" % publickey,
                            use_bash=True)
        except:
            slave_shell.run("echo -e '%s' >> .ssh/authorized_keys" % publickey)
            ip_addr = slave_shell.get_ip_addr()
            nn_shell.run("ssh-keyscan -H %s >> .ssh/known_hosts" % ip_addr)

def format_namenode(shell):
    try:
        # Formatting should be done only once. So check for indicator file
        shell.run("ls formatting_done")
    except:
        shell.run_hadoop_cmd("hadoop namenode -format")
        # Create indicator file so that we don't end up formatting again
        shell.run("touch formatting_done")

def run_TestDFSIO(shell, result_file="results.out", test_type="write",
                  number_of_files=1, file_size='1MB'):
    """
    Run TestDFSIO with various options. Assumes that start_all has been run
    """
    dfsio_jar = ("software/hadoop-3.0.0-alpha1/share/hadoop/mapreduce/"
                 "hadoop-mapreduce-client-jobclient-3.0.0-alpha1-tests.jar")
    cmd = "yarn jar %s TestDFSIO" % dfsio_jar
    cmd += " -resFile %s" % result_file
    cmd += " -%s" % test_type
    cmd += " -nrFiles %s" % number_of_files
    cmd += " -size %s" % file_size

    result = shell.run_hadoop_cmd(cmd)
    return result.output

def save_output(output, filename):
    """Saves the string output to a file in the `output` directory"""
    path = os.path.join("output", filename)
    open(path, "w").write(output)

def setup_hadoop_testbase(namenode, resourcemgr, slave0, slave1, slave2):
    # Assume all entries are VMs and have ports embedded
    nn_hostname, nn_port = namenode.split(':')
    nn_hostname += ".wisc.cloudlab.us"
    nn_shell = create_ssh_shell(nn_hostname, port=int(nn_port))
    master_ip = nn_shell.get_ip_addr()

    rm_hostname, rm_port = resourcemgr.split(':')
    rm_hostname += ".wisc.cloudlab.us"
    rm_shell = create_ssh_shell(rm_hostname, port=int(rm_port))

    # NOTE: We are not running a master and slave on the same instance
    slave_ip_addrs = []

    slave0_hostname, slave0_port = slave0.split(':')
    slave0_hostname += ".wisc.cloudlab.us"
    slave0_shell = create_ssh_shell(slave0_hostname, port=int(slave0_port))
    slave_ip_addrs += [slave0_shell.get_ip_addr()]

    slave1_hostname, slave1_port = slave1.split(':')
    slave1_hostname += ".wisc.cloudlab.us"
    slave1_shell = create_ssh_shell(slave1_hostname, port=int(slave1_port))
    slave_ip_addrs += [slave1_shell.get_ip_addr()]

    slave2_hostname, slave2_port = slave2.split(':')
    slave2_hostname += ".wisc.cloudlab.us"
    slave2_shell = create_ssh_shell(slave2_hostname, port=int(slave2_port))
    slave_ip_addrs += [slave2_shell.get_ip_addr()]

    setup_passwordless(nn_shell, [slave0_shell, slave1_shell, slave2_shell])

    for shell in [nn_shell, rm_shell, slave0_shell,
                  slave1_shell, slave2_shell]:
        # XXX: Could be done in parallel
        setup_hadoop(shell, master_ip, slave_ip_addrs)

    format_namenode(nn_shell)

    nn_shell.run_hadoop_cmd("stop_all", allow_error=True)

    return nn_shell
