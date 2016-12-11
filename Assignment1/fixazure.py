#!/bin/python
import argparse
import os
import sys
import subprocess
import paramiko
from multiprocessing import Pool
import itertools
import logging

def exec_command(vmname, ssh, cmd, passwordRequired=False):
    status, output_lines, error_lines = block_exec(ssh, cmd, passwordRequired)
    error = "SUCCESS" if status == 0 else "FAIL"
    print "[%s] Status: %s - %s" % (vmname, error, cmd)
    print output_lines, error_lines
    return output_lines, error_lines


def block_exec(ssh, cmd, passwordRequired=False):
    stdin, stdout, stderr = ssh.exec_command(cmd)
    if passwordRequired:
        stdin.write(args.password + "\n")
        stdin.flush()
    exit_status = stdout.channel.recv_exit_status()
    return exit_status, stdout.readlines(), stderr.readlines()


class SSHConnections(object):# {{{

    def __init__(self, ):
        self.connections = {}

    def get_ssh(self, n, publicip=None):
        if n in self.connections:
            logging.info("Using existing SSH connection to %s" % n)
            return self.connections[n]
        else:
            assert(publicip is not None)
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print publicip, args.user, args.password
            ssh.connect(publicip, username=args.user, password=args.password)
            self.connections[n] = ssh
            logging.info("Created a new SSH connection to %s" % n)
            return ssh

    def close_ssh(self, n):
        if n in self.connections:
            self.connections[n].close()
        else:
            logging.info("No open connections for %s" % n)

    def close_all(self, ):
        for conn in self.connections.values():
            conn.close()# }}}

# start VMs after deallocation:
def start_vms():
    for i in range(1, 1 + args.numberofvms):
        cmd = "azure vm start group%d vm%d" % (args.groupnumber, i)
        print cmd
        p = subprocess.Popen(cmd, shell=True, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output_lines = []
        if p.stdout is not None:
            print "Waiting for vm%d start to complete...." % i
            output_lines = p.stdout.readlines()
        output_lines = [x for x in output_lines if "error" in x]

        if len(output_lines) > 0:
            print "ERROR: Something went wrong while starting vm%d. " % i,
            print "Here is the output from the command"
            print "".join(output_lines)
            return False
        else:
            print "Starting vm%d successful." % i


# reset your azure access keys
def reset_access():# {{{
    """
    Reset the access for each of the VMs in the cluster. This is required
    because after re-allocating VMs the SSH daemons fails.
    """
    print "Delete all entries from known_hosts pertaining to Azure vms"
    khclear_cmd = "sed -i.bak '/cs838/d' ~/.ssh/known_hosts"
    print khclear_cmd
    os.system(khclear_cmd) # clear the known_hosts

    for i in range(1, 1 + args.numberofvms):
        cmd = "azure vm reset-access -g group%d vm%d -u %s -p %s" % (args.groupnumber, i, args.user, args.password)
        print cmd
        p = subprocess.Popen(cmd, shell=True, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output_lines = []
        if p.stdout is not None:
            print "Waiting for vm%d reset to complete...." % i
            output_lines = p.stdout.readlines()
        output_lines = [x for x in output_lines if "error" in x]

        if len(output_lines) > 0:
            print "ERROR: Something went wrong while resetting vm%d's access. " % i,
            print "Here is the output from the command"
            print "".join(output_lines)
            return False
        else:
            print "Resetting vm%d successful." % i

    return True# }}}


def _get_disk_device_mapping(lines):# {{{
    """
    Parse the output of sudo fdisk -l and return the device mapping
    """
    d = dict()
    for line in lines:
        if not line.strip().startswith("Disk /dev"):
            continue
        parts = line.strip().split()
        print parts[1], parts[2]
        disk_size = float(parts[2])
        if disk_size > 40.0 and disk_size < 75.0:
            d["/workspace"] = parts[1][:-1] + '1'
        elif disk_size > 100.0:
            d["/mnt"] = parts[1][:-1] + '1'
    return d# }}}


def _get_fdisk_output_through_ssh(vmname, ssh):# {{{
    """
    Executes fdisk using SSH and gather the output.
    """
#    output, error = exec_command(vmname, ssh, "ls -l /etc")
#    output, error = exec_command(vmname, ssh, "sudo apt-get install vim")
    output, error = exec_command(vmname, ssh, "sudo -S -p '' fdisk -l", True)
    return output# }}}


def _get_fdisk_output_from_file():# {{{
    """
    Get lines from the sample fdisk file
    """
    lines = []
    with open("fdisk.out") as fp:
        lines = fp.readlines()
    return lines# }}}


def _fix_disk_per_vm(vmname, publicip):# {{{
    """
    Fix all the disk issues per VM.
    """
    # 1. create a ssh object
    ssh = cnx.get_ssh(vmname, publicip)
    # 2. get the output of "sudo fdisk -l"
#    fdisk_output = _get_fdisk_output_from_file()
    fdisk_output = _get_fdisk_output_through_ssh(vmname, ssh)
    # 3. Figure out which disk maps to which device
    disk_device_map = _get_disk_device_mapping(fdisk_output)
    print disk_device_map
    # 4. create the appropriate directories
    exec_command(vmname, ssh, "sudo -S -p '' mkdir -p /workspace", True)
    exec_command(vmname, ssh, "sudo -S -p '' mkdir -p /mnt", True)
    # 5. unmount them if already mounted
    for v in disk_device_map.values():
        exec_command(vmname, ssh, "sudo -S -p '' umount %s" % (v), True)
    # 6. now mount them properly
    for k, v in disk_device_map.iteritems():
        exec_command(vmname, ssh, "sudo -S -p '' mount %s %s" % (v,k), True)

    # 7. create the appropriate directories
    exec_command(vmname, ssh, "sudo -S -p '' mkdir -p /mnt/logs/apps", True)
    exec_command(vmname, ssh, "sudo -S -p '' mkdir -p /mnt/logs/hadoop", True)

    # 8. change the ownership of the directories
    for k in disk_device_map.keys():
        exec_command(vmname, ssh, "sudo -S -p '' chown -R %s:%s %s" % (args.user, args.user, k), True)
    return# }}}


def fix_the_disks():# {{{
    """
    Go over each VM and call the function required to identify the disk
    attached to each of them and mount them appropriately. The called function
    also assigns the appropriate permissions for the directories.
    """
#    publicips = _read_public_ips_from_azure_cli()
    publicips = _generate_public_hostnames()

    retval = True
    for name, ip in publicips.iteritems():
        retval = _fix_disk_per_vm(name, ip)
        if retval is False:
            return False
    return True# }}}


def _read_public_ips_from_file():
    with open(args.fpublicips) as fp:
        ips = {x.strip().split(':')[0]: x.strip().split(':')[1] for x in fp.readlines()}
    return ips


def _generate_public_hostnames():
    d = dict()
    for i in range(1, 1 + args.numberofvms):
        d["vm%d" % i] = "cs838fall2016group%d%d.eastus.cloudapp.azure.com" % (args.groupnumber, i)
    return d


def _read_public_ips_from_azure_cli():
    """
    Return a dictionary with vm name to its public ip mapping
    """
    d = dict()
    return d

def _copy_public_keys(vmname, publicip, public_key):
    ssh = cnx.get_ssh(vmname, publicip)
    cmd = "echo '%s' > ~/.ssh/authorized_keys" % public_key
    exec_command(vmname, ssh, cmd)
    return

def _setup_vm_keys(vmname, publicip):
    # 1. create a ssh object
    ssh = cnx.get_ssh(vmname, publicip)
    # 2. generate the keys
    exec_command(vmname, ssh, "ssh-keygen -f /home/ubuntu/.ssh/id_rsa -t rsa -N ''")
    # 3. get the values of public keys
    public_key_lines, _ = exec_command(vmname, ssh, "cat ~/.ssh/id_rsa.pub")
    print "".join(public_key_lines)
    # 4. append it to slave machines
    public_key = "".join([x.strip() for x in public_key_lines])
    return public_key

def _clear_keys(vmname, publicip):
    ssh = cnx.get_ssh(vmname, publicip)
    exec_command(vmname, ssh, "rm -f ~/.ssh/id_rsa")
    exec_command(vmname, ssh, "rm -f ~/.ssh/id_rsa.pub")
    exec_command(vmname, ssh, "rm -f ~/.ssh/authorized_keys")


def reset_passwordless_ssh():
    publicips = _generate_public_hostnames()

    for name, ip in publicips.iteritems():
        _clear_keys(name, ip)

    mastername = "vm%d" % args.master
    public_key = _setup_vm_keys(mastername, publicips[mastername])
    for i in range(1, 1 + args.numberofvms):
        vmid = "vm%d" % i
        _copy_public_keys(vmid, publicips[vmid], public_key)
    return


def main():
    retval = True
    #0. Start VMs
    retval = start_vms()
    if retval is False:
        return
    # 1. Reset the azure password
    retval = reset_access()
    if retval is False:
        return
    # 2. Attach the disks properly
    retval = fix_the_disks()
    if retval is False:
        return
    # 3. Reset the ssh keys for passwordless ssh
    reset_passwordless_ssh()
    if retval is False:
        return

    return


def cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument('groupnumber', type=int) # positional argument
    parser.add_argument('--numberofvms', '-n', type=int, default=4,
                        help="The number of vms in your setup.")
    parser.add_argument('--fpublicips', '-f', type=str, default='publicips.txt',
                        help="A file containing the public ips. Used only for testing.")
    parser.add_argument('--password', '-p', type=str, default='Ubuntu123$',
                        help="The password set for the VMs")
    parser.add_argument('--user', '-u', type=str, default='ubuntu',
                        help="The user name for the logging into VMs.")
    parser.add_argument('--master', '-m', type=int, default=1)
    return parser.parse_args()


if __name__ == "__main__":
    args = cmdline()
    cnx = SSHConnections()
    print args.groupnumber
    main()
