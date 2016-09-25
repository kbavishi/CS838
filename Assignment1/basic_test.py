#! /usr/bin/python

import datetime
import json
import os
import re
import subprocess
import time
import shlex
from collections import OrderedDict
from os.path import isfile

# Disk sector is 512B
sector_size = 512

def check_output(cmd):
    if type(cmd) == str:
        cmd = shlex.split(cmd)
    return subprocess.check_output(cmd)

def fetch_jhist_files(expected_count):
    date = datetime.datetime.today().strftime( "%Y/%m/%d" )
    hdfs_path = "/tmp/hadoop-yarn/staging/history/done/%s/000000/*.jhist" % date
    # Keep retrying till jhist files appear. HDFS takes a while
    while os.system("hadoop fs -ls %s" % hdfs_path):
        time.sleep(1)

    # Keep retrying till all jhist files appear
    print 'EXPECTING COUNT', expected_count
    while True:
        output = check_output("hadoop fs -ls %s" % hdfs_path).split("\n")
        output = filter(lambda l: not l.startswith("SLF4J"), output)
        if len(output) == expected_count:
            break
        else:
            time.sleep(1)

    os.system("hadoop fs -copyToLocal %s /home/ubuntu/output/" % hdfs_path)
    os.system("hadoop fs -rm %s" % hdfs_path)

def parse_jhist_file(filename):
    lines = open(filename, "r").readlines()
    # First two lines are useless - Title and schema stuff
    lines = lines[2:]

    # List of JSON objects
    result = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        result += [json.loads(line)]
    return result

def get_task_events(filename):
    result = parse_jhist_file(filename)
    is_task = lambda event: (event["type"] == "TASK_STARTED" or
                             event["type"] == "TASK_FAILED" or
                             event["type"] == "TASK_FINISHED")
    task_events = filter(is_task, result)
    final_output = []
    for task in task_events:
        # Example: (23, TASK_STARTED, MAP)
        if ('startTime' not in task['event'].values()[0] and
            'finishTime' not in task['event'].values()[0]):
            print 'STRANGE TASK FOUND'
            print task
            continue

        if task['type'] == 'TASK_STARTED':
            x = task['event'].values()[0]['startTime']
        else:
            x = task['event'].values()[0]['finishTime']

        final_output += [(x, task['type'],
                          task['event'].values()[0]['taskType'])]
    return final_output

def get_expected_jhist_count(output_dir, query_type='mr'):
    path = os.path.join(output_dir, "query_%s.out" % query_type)
    text = open(path, "r").read()
    import pdb; pdb.set_trace()
    return len(re.findall("Starting Job", text))

def get_all_task_events():
    output_dir = "/home/ubuntu/output"
    count = get_expected_jhist_count(output_dir)
    fetch_jhist_files(count)
    all_files = [ os.path.join(output_dir, f) for f in os.listdir(output_dir) ]
    all_jhists = [ f for f in all_files if isfile(f) and f.endswith("jhist") ]
    output = []
    for jhist in all_jhists:
        output += get_task_events(jhist)
    output = sorted(output)

    map_tasks, reduce_tasks = 0, 0
    for (_, event_type, task_type) in output:
        if event_type == 'TASK_STARTED':
            if task_type == 'MAP':
                map_tasks += 1
            else:
                reduce_tasks += 1

    return output, map_tasks, reduce_tasks

def get_netstats(vm):
    # face |bytes    packets errs drop fifo frame compressed multicast|bytes
    # packets errs drop fifo colls carrier compressed
    output = subprocess.check_output(shlex.split("ssh %s cat /proc/net/dev" % vm))
    lines = output.split("\n")
    for line in lines:
        line = line.strip()
        if line.startswith("eth0"):
            stats = line.split()
            # Return rxBytes, txBytes
            return int(stats[1]), int(stats[9])
    raise Exception("Counters not found")

def get_all_netstats():
    # Assume that vm1 is the master
    rx_bytes, tx_bytes = 0, 0
    for vm in ["vm2", "vm3", "vm4"]:
        rx, tx = get_netstats(vm)
        rx_bytes += rx
        tx_bytes += tx
    return rx_bytes, tx_bytes

def find_disk(vm):
    # Get mountpoints
    paths = subprocess.check_output(shlex.split("ssh %s df -H" % vm)).split("\n")
    for path in paths:
        if "workspace" in path:
            # We have found it
            return path.split()[0].strip("/dev/")
    raise Exception("No disk found")

def get_diskstats(vm):
    diskname = find_disk(vm)
    output = subprocess.check_output(shlex.split("ssh %s cat /proc/diskstats" % vm))
    lines = output.split("\n")
    for line in lines:
        if diskname in line:
            stats = line.split()
            # Return sectors read, sectors written multiplied by disk sector
            # size
            return int(stats[5]) * sector_size, int(stats[9]) * sector_size
    raise Exception("Counters not found")

def get_all_diskstats():
    # Assume that vm1 is the master
    read_bytes, write_bytes = 0, 0
    for vm in ["vm2", "vm3", "vm4"]:
        read, write = get_diskstats(vm)
        read_bytes += read
        write_bytes += write
    return read_bytes, write_bytes

def run_mr_query(query_num):
    # Clear cache buffers
    for vm in ["vm2", "vm3", "vm4"]:
        os.system("""ssh %s sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches" """
                  % vm)

    cmd = "(hive --hiveconf hive.execution.engine=mr -f "\
    "/home/ubuntu/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%d.sql "\
    "--database tpcds_text_db_1_50) 2> output/query_mr.out" % query_num

    print "About to run MR query", query_num

    start_time = time.time()
    rx_bytes_start, tx_bytes_start = get_all_netstats()
    read_bytes_start, write_bytes_start = get_all_diskstats()

    os.system(cmd)

    end_time = time.time()
    rx_bytes_end, tx_bytes_end = get_all_netstats()
    read_bytes_end, write_bytes_end = get_all_diskstats()

    print "Finished running MR query", query_num

    results = OrderedDict()
    results["run_time"] = end_time - start_time
    results["rx_bytes"] = rx_bytes_end - rx_bytes_start
    results["tx_bytes"] = tx_bytes_end - tx_bytes_start
    results["read_bytes"] = read_bytes_end - read_bytes_start
    results["write_bytes"] = write_bytes_end - write_bytes_start

    timeline, map_tasks, reduce_tasks = get_all_task_events()

    return results, map_tasks, reduce_tasks

def run_tez_query(query_num):
    # Clear cache buffers
    os.system("""sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches" """)

    cmd = "(hive --hiveconf hive.execution.engine=tez --hiveconf "\
    "hive.tez.container.size=$containerSize --hiveconf "\
    "hive.tez.java.opts=$containerJvm -f "\
    "/home/ubuntu/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%d.sql "\
    "--database tpcds_text_db_1_50) 2> output/query_tez.out" % query_num

    print "About to run Tez query", query_num

    start_time = time.time()
    rx_bytes_start, tx_bytes_start = get_all_netstats()
    read_bytes_start, write_bytes_start = get_all_diskstats()

    os.system(cmd)

    end_time = time.time()
    rx_bytes_end, tx_bytes_end = get_all_netstats()
    read_bytes_end, write_bytes_end = get_all_diskstats()

    print "Finished running MR query", query_num

    results = OrderedDict()
    results["run_time"] = end_time - start_time
    results["rx_bytes"] = rx_bytes_end - rx_bytes_start
    results["tx_bytes"] = tx_bytes_end - tx_bytes_start
    results["read_bytes"] = read_bytes_end - read_bytes_start
    results["write_bytes"] = write_bytes_end - write_bytes_start
    return results

def main():
    #for query in [12, 21, 50, 71, 85]:
    for query in [12]:
        results, map_tasks, reduce_tasks = run_mr_query(query)
        print results
        print map_tasks, reduce_tasks
        #results = run_tez_query(query)

if __name__ == '__main__':
    main()
