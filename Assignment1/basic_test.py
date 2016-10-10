#! /usr/bin/python

import threading
import datetime
import itertools
import json
import os
import pexpect
import re
import shlex
import subprocess
import sys
import threading
import time
from collections import OrderedDict, namedtuple
from os.path import isfile
from graphviz import Digraph

# Disk sector is 512B
sector_size = 512

slaves = ["vm1", "vm2", "vm3", "vm4"]

output_dir = "/home/ubuntu/output"
tcpds_workload_dir = os.path.join("/home/ubuntu/workload",
                                  "hive-tpcds-tpch-workload")

mr_output_file = "final_output_mr.txt"
tez_output_file = "final_output_tez.txt"

JobStat = namedtuple("JobStat", ["job_id", "hdfs_bytes_read",
                                 "hdfs_bytes_written", "map_num", "reduce_num"])

def check_output(cmd):
    if type(cmd) == str:
        cmd = shlex.split(cmd)
    return subprocess.check_output(cmd)

def drop_caches(vm):
    child = pexpect.spawn("ssh -t %s sudo bash drop_caches.sh" % vm)
    child.expect(".sudo. password")
    child.sendline("Ubuntu123$")
    child.expect(pexpect.EOF)

def fetch_jhist_files(expected_count, job_id):
    date = datetime.datetime.today().strftime( "%Y/%m/%d" )
    jhist_hdfs_path = "/tmp/hadoop-yarn/staging/history/done/%s/000000/*%s*.jhist" % (date, job_id)
    xml_hdfs_path = "/tmp/hadoop-yarn/staging/history/done/%s/000000/*%s*.xml" % (date, job_id)
    # Keep retrying till jhist files appear. HDFS takes a while
    while os.system("hadoop fs -ls %s" % jhist_hdfs_path):
        print 'JHIST FILES YET TO APPEAR'
        time.sleep(2)

    # Keep retrying till all jhist files appear
    print 'EXPECTING COUNT', expected_count
    while True:
        output = check_output("hadoop fs -ls %s" % jhist_hdfs_path).split("\n")
        output = filter(lambda l: l and not l.startswith("SLF4J"), output)
        if len(output) == expected_count:
            break
        else:
            print 'ALL JHIST FILES YET TO APPEAR'
            time.sleep(2)

    os.system("hadoop fs -copyToLocal %s /home/ubuntu/output/" % jhist_hdfs_path)
    os.system("hadoop fs -rm %s" % jhist_hdfs_path)
    os.system("hadoop fs -copyToLocal %s /home/ubuntu/output/" % xml_hdfs_path)
    os.system("hadoop fs -rm %s" % xml_hdfs_path)

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
    timeline = []
    map_tasks, reduce_tasks = 0, 0
    for task in task_events:
        # Example: (23, TASK_STARTED, MAP)
        info = task['event'].values()[0]

        if ('startTime' not in info and 'finishTime' not in info):
            print 'STRANGE TASK FOUND'
            print task
            continue

        if task['type'] == 'TASK_STARTED':
            t = info['startTime']
        else:
            t = info['finishTime']

        timeline += [(t, task['type'], info['taskType'])]
        if task['type'] == 'TASK_STARTED' and info['taskType'] == 'MAP':
            map_tasks += 1
        elif task['type'] == 'TASK_STARTED' and info['taskType'] == 'REDUCE':
            reduce_tasks += 1

    return timeline, map_tasks, reduce_tasks

def get_job_stats(filename):
    result = parse_jhist_file(filename)
    job_id, hdfs_bytes_read, hdfs_bytes_written = 0, 0, 0
    for task in result:
        if task["type"] == "JOB_FINISHED":
            info = task['event'].values()[0]
            job_id = info['jobid']
            hdfs_bytes_read = info["totalCounters"]["groups"][0]["counts"][5]["value"]
            hdfs_bytes_written = info["totalCounters"]["groups"][0]["counts"][6]["value"]
            break
    return job_id, hdfs_bytes_read, hdfs_bytes_written

def get_expected_jhist_count():
    os.system("sync")
    path = os.path.join(output_dir, "query_mr.out")
    text = open(path, "r").read()
    x = re.search( "Starting Job = (\S*)", text )
    job_id = x.group(1).strip(',').split('_')[1]
    return len(re.findall("Starting Job", text)), job_id

def parse_xml_file(xml_file):
    lines = check_output("grep adjacency %s" % xml_file).split("\n")
    xml_graph = {}
    for line in lines:
        if not line:
            continue
        pattern = "(Stage-\d*)"
        stages = re.findall(pattern, line)
        assert len(stages) >= 2
        key = stages[0]
        xml_graph[key] = {}
        for value in stages[1:]:
            xml_graph[key][value] = True
    return xml_graph

def get_all_mr_task_events():
    count, job_id = get_expected_jhist_count()
    fetch_jhist_files(count, job_id)
    all_files = [ os.path.join(output_dir, f) for f in os.listdir(output_dir) ]
    all_jhists = [ f for f in all_files if isfile(f) and f.endswith("jhist") ]
    timeline = []
    job_stats = []
    for jhist in all_jhists:
        tl, map_num, reduce_num = get_task_events(jhist)
        timeline.extend(tl)
        job_id, hdfs_bytes_read, hdfs_bytes_written = get_job_stats(jhist)
        job_stats += [JobStat(job_id, hdfs_bytes_read, hdfs_bytes_written,
                              map_num, reduce_num)]
    timeline = sorted(timeline)

    all_xmls = [ f for f in all_files if isfile(f) and f.endswith("xml") ]
    xml = all_xmls[0]
    xml_graph = parse_xml_file(xml)

    map_tasks, reduce_tasks = 0, 0
    for (_, event_type, task_type) in timeline:
        if event_type == 'TASK_STARTED':
            if task_type == 'MAP':
                map_tasks += 1
            else:
                reduce_tasks += 1

    return timeline, job_stats, xml_graph, map_tasks, reduce_tasks

def almost_equal(value1, value2):
    tolerance = 0.01
    return value1*(1-tolerance) <= value2 <= value1*(1+tolerance)

def draw_graph(job_stats):
    # Graph stores children of a node.
    # Eg: graph["A"] = "B" means that B is a child of A.
    graph = {}
    for job1, job2 in itertools.combinations(job_stats, 2):
        # CASE A: Compare HDFS bytes read by job1 to HDFS bytes written by job2. If
        # equal, it means job2 is parent.
        # CASE B: Compare HDFS bytes written by job1 to HDFS bytes read by job2. If
        # equal, it means job1 is parent.
        if almost_equal(job1.hdfs_bytes_read, job2.hdfs_bytes_written):
            # CASE A
            print "%s -> %s" % (job2.job_id, job1.job_id)
            graph[job2.job_id] = job1.job_id
        elif almost_equal(job1.hdfs_bytes_written, job2.hdfs_bytes_read):
            # CASE B
            print "%s -> %s" % (job1.job_id, job2.job_id)
            graph[job1.job_id] = job2.job_id
    return graph

def get_netstats(vm):
    # face |bytes    packets errs drop fifo frame compressed multicast|bytes
    # packets errs drop fifo colls carrier compressed
    output = check_output("ssh %s cat /proc/net/dev" % vm)
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
    for vm in slaves:
        rx, tx = get_netstats(vm)
        rx_bytes += rx
        tx_bytes += tx
    return rx_bytes, tx_bytes

def find_disk(vm):
    # Get mountpoints
    paths = check_output("ssh %s df -H" % vm).split("\n")
    for path in paths:
        if "workspace" in path:
            # We have found it
            return path.split()[0].strip("/dev/")
    raise Exception("No disk found")

def get_diskstats(vm):
    diskname = find_disk(vm)
    output = check_output("ssh %s cat /proc/diskstats" % vm)
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
    for vm in slaves:
        read, write = get_diskstats(vm)
        read_bytes += read
        write_bytes += write
    return read_bytes, write_bytes

def run_mr_query(query_num):
    # Clear previous HDFS jhist files
    date = datetime.datetime.today().strftime( "%Y/%m/%d" )
    jhist_hdfs_path = "/tmp/hadoop-yarn/staging/history/done/%s/000000/*.jhist" % date
    xml_hdfs_path = "/tmp/hadoop-yarn/staging/history/done/%s/000000/*.xml" % date
    os.system("hadoop fs -rm %s" % jhist_hdfs_path)
    os.system("hadoop fs -rm %s" % xml_hdfs_path)
    # Clear local jhist files
    os.system("rm -rf output/*.jhist")

    # Clear cache buffers
    for vm in slaves:
        drop_caches(vm)

    query_output = os.path.join(output_dir, "query_mr.out")
    query_path = os.path.join(tcpds_workload_dir,
                              "sample-queries-tpcds/query%d.sql" % query_num)

    cmd = "(hive --hiveconf hive.execution.engine=mr "\
    "-f %s --database tpcds_text_db_1_50) 2> %s" % (query_path, query_output)

    print "About to run MR query", query_num

    start_time = time.time()
    rx_bytes_start, tx_bytes_start = get_all_netstats()
    read_bytes_start, write_bytes_start = get_all_diskstats()

    # Start a subprocess to follow the query output
    os.system("touch %s" % query_output)
    proc = subprocess.Popen(shlex.split("tail -f %s" % query_output))
    # Run the actual query command
    os.system(cmd)
    # Kill the subprocess following query output. Not needed anymore
    proc.kill()

    end_time = time.time()
    rx_bytes_end, tx_bytes_end = get_all_netstats()
    read_bytes_end, write_bytes_end = get_all_diskstats()

    print "Finished running MR query", query_num

    results = OrderedDict()
    results["query"] = query_num
    results["run_time"] = end_time - start_time
    results["rx_bytes"] = rx_bytes_end - rx_bytes_start
    results["tx_bytes"] = tx_bytes_end - tx_bytes_start
    results["read_bytes"] = read_bytes_end - read_bytes_start
    results["write_bytes"] = write_bytes_end - write_bytes_start

    timeline, job_stats, xml_graph, map_tasks, reduce_tasks = get_all_mr_task_events()
    results["map_tasks"] = map_tasks
    results["reduce_tasks"] = reduce_tasks

    graph = draw_graph(job_stats)
    os.system("rm -rf output/*.jhist")

    return results, timeline, job_stats, graph, xml_graph

def parse_tez_hist_file(filename):
    lines = open(filename, "r").readlines()
    lines = lines[1:]
    # List of JSON objects
    result = []
    for line in lines:
        line = line.strip().strip("\x01")
        if not line:
            continue
        result += [json.loads(line)]
    return result

def get_tez_events(filename):
    result = parse_tez_hist_file(filename)
    is_vertex = lambda event: (event["events"][0]["eventtype"] ==
                               "VERTEX_FINISHED")
    vertex_events = filter(is_vertex, result)
    job_stats = []
    job_timeline = []
    for vertex in vertex_events:
        job_id = vertex['entity']
        for counters in vertex['otherinfo']['counters']['counterGroups']:
            if 'FileSystemCounter' in counters['counterGroupName']:
                fs_counters = counters['counters']
            elif 'TaskCounter' in counters['counterGroupName']:
                task_counters = counters['counters']
            elif 'HIVE' in counters['counterGroupName']:
                hive_counters = counters['counters']

        hdfs_bytes_read, hdfs_bytes_written = 0, 0
        file_bytes_read, file_bytes_written = 0, 0
        for counter in fs_counters:
            if counter['counterName'] == 'HDFS_BYTES_READ':
                hdfs_bytes_read = counter['counterValue']
            elif counter['counterName'] == 'HDFS_BYTES_WRITTEN':
                hdfs_bytes_written = counter['counterValue']
            elif counter['counterName'] == 'FILE_BYTES_READ':
                file_bytes_read = counter['counterValue']
            elif counter['counterName'] == 'FILE_BYTES_WRITTEN':
                file_bytes_written = counter['counterValue']

        merge_phase_time, shuffle_phase_time = 0.0, 0.0
        shuffle_bytes = 0
        for counter in task_counters:
            if counter['counterName'] == 'MERGE_PHASE_TIME':
                merge_phase_time = counter['counterValue'] * 1./1000
            elif counter['counterName'] == 'SHUFFLE_PHASE_TIME':
                shuffle_phase_time = counter['counterValue'] * 1./1000
            elif counter['counterName'] == 'SHUFFLE_BYTES':
                shuffle_bytes = counter['counterValue']

        job_time = vertex['otherinfo']['timeTaken'] * 1./1000

        job_stats += [(job_id, hdfs_bytes_read, hdfs_bytes_written,
                       file_bytes_read, file_bytes_written,
                       job_time, shuffle_phase_time, merge_phase_time,
                       shuffle_bytes, hive_counters)]
        job_timeline += [(vertex['otherinfo']['stats']['firstTaskStartTime'],
                          "JOB_STARTED", job_id)]
        job_timeline += [(vertex['otherinfo']['stats']['lastTaskFinishTime'],
                          "JOB_FINISHED", job_id)]

    job_timeline = sorted(job_timeline)

    is_task = lambda event: event["events"][0]["eventtype"] == "TASK_FINISHED"
    task_events = filter(is_task, result)

    timeline = []
    non_aggregators, hdfs_readers, aggregators = 0, 0, 0
    for task in task_events:
        # Build a timeline of events, where each event looks like:
        # (23, TASK_STARTED, MAP)
        start_time = task['otherinfo']['startTime']
        end_time = task['otherinfo']['endTime']
        
        counters = task['otherinfo']['counters']['counterGroups'][1]['counters']
        file_bytes_read, hdfs_bytes_read = 0, 0
        for counter in counters:
            if counter['counterName'] == 'FILE_BYTES_READ':
                file_bytes_read = counter['counterValue']
            elif counter['counterName'] == 'HDFS_BYTES_READ':
                hdfs_bytes_read = counter['counterValue']

        if hdfs_bytes_read != 0:
            hdfs_readers += 1
            task_type = 'HDFS_READER'
            timeline += [(start_time, 'TASK_STARTED', task_type)]
            timeline += [(end_time, 'TASK_FINISHED', task_type)]

            if file_bytes_read == 0:
                non_aggregators += 1
                task_type = 'NON_AGGREGATOR'
                timeline += [(start_time, 'TASK_STARTED', task_type)]
                timeline += [(end_time, 'TASK_FINISHED', task_type)]

        if file_bytes_read != 0:
            aggregators += 1
            task_type = 'AGGREGATOR'
            timeline += [(start_time, 'TASK_STARTED', task_type)]
            timeline += [(end_time, 'TASK_FINISHED', task_type)]

    return (timeline, job_stats, job_timeline,
            hdfs_readers, non_aggregators, aggregators)


def get_tez_events_old(filename):
    result = parse_tez_hist_file(filename)

    # We can only find out the task type (MAP/REDUCE) by finding the type of the
    # vertex it corresponds to. We first iterate through all VERTEX_INITIALIZED
    # events and note down the task type. Later while iterating over the
    # TASK_FINISHED messages, we use the vertex ID to determine the task type
    is_vertex = lambda event: event["events"][0]["eventtype"] == "VERTEX_INITIALIZED"
    vertex_events = filter(is_vertex, result)

    # Stores ID to task type mapping
    vertex_to_task_type = {}
    for vertex in vertex_events:
        vertex_id = vertex["entity"].strip("vertex_")
        vertex_name = vertex["otherinfo"]["vertexName"]
        if "Map" in vertex_name:
            vertex_to_task_type[vertex_id] = "MAP"
        elif "Reducer" in vertex_name:
            vertex_to_task_type[vertex_id] = "REDUCE"
        else:
            assert False, "Unrecognized vertex type"

    is_task = lambda event: event["events"][0]["eventtype"] == "TASK_FINISHED"
    task_events = filter(is_task, result)

    timeline = []
    map_tasks, reduce_tasks = 0, 0
    for task in task_events:
        # Build a timeline of events, where each event looks like:
        # (23, TASK_STARTED, MAP)
        start_time = task['otherinfo']['startTime']
        end_time = task['otherinfo']['endTime']

        # Use the vertex ID to determine the task type (map/reduce)
        rindex = task['entity'].strip('task_').rindex('_')
        vertex_id = task['entity'].strip('task_')[:rindex]
        task_type = vertex_to_task_type[vertex_id]

        timeline += [(start_time, 'TASK_STARTED', task_type)]
        timeline += [(end_time, 'TASK_FINISHED', task_type)]
        if task_type == 'MAP':
            map_tasks += 1
        else:
            reduce_tasks += 1

    return timeline, map_tasks, reduce_tasks

def fetch_tez_history():
    hdfs_path = "/tmp/tez-history/*"
    # Keep retrying till history files appear. HDFS takes a while
    while os.system("hadoop fs -ls %s" % hdfs_path):
        print 'HISTORY FILES YET TO APPEAR'
        time.sleep(2)

    # Keep retrying till all jhist files appear
    expected_count = 1
    print 'EXPECTING COUNT', expected_count
    while True:
        output = check_output("hadoop fs -ls %s" % hdfs_path).split("\n")
        output = filter(lambda l: l and not l.startswith("SLF4J"), output)
        if len(output) == expected_count:
            break
        else:
            print 'ALL HISTORY FILES YET TO APPEAR'
            time.sleep(2)

    os.system("hadoop fs -copyToLocal %s /home/ubuntu/output/" % hdfs_path)
    os.system("hadoop fs -rm %s" % hdfs_path)

def get_all_tez_task_events():
    fetch_tez_history()
    all_files = [ os.path.join(output_dir, f) for f in os.listdir(output_dir) ]
    all_hists = [ f for f in all_files if isfile(f) and "history" in f ]
    assert len(all_hists) == 1, "Should be only one history file"
    timeline = []
    job_stats = []
    hist = all_hists[0]

    timeline, job_stats, job_timeline, hdfs_readers, non_aggregators, aggregators = get_tez_events(hist)
    timeline = sorted(timeline)
    job_timeline = sorted(job_timeline)

    return timeline, job_stats, job_timeline, hdfs_readers, non_aggregators, aggregators, hist

def run_tez_query(query_num):
    # Clear previous HDFS history files
    os.system("hadoop fs -rm /tmp/tez-history/*")
    # Clear local history files
    os.system("rm -rf output/history*")

    # Clear cache buffers
    for vm in slaves:
        drop_caches(vm)

    query_output = os.path.join(output_dir, "query_tez.out")
    query_path = os.path.join(tcpds_workload_dir,
                              "sample-queries-tpcds/query%d.sql" % query_num)

    cmd = "(hive --hiveconf hive.execution.engine=tez "\
    "--hiveconf hive.tez.container.size=4800 "\
    "--hiveconf hive.tez.java.opts=-Xmx4600m "\
    "-f %s --database tpcds_text_db_1_50) 2> %s" % (query_path, query_output)

    print "About to run Tez query", query_num

    start_time = time.time()
    rx_bytes_start, tx_bytes_start = get_all_netstats()
    read_bytes_start, write_bytes_start = get_all_diskstats()

    # Start a subprocess to follow the query output
    os.system("touch %s" % query_output)
    proc = subprocess.Popen(shlex.split("tail -f %s" % query_output))
    # Run the actual query command
    os.system(cmd)
    # Kill the subprocess following query output. Not needed anymore
    proc.kill()

    end_time = time.time()
    rx_bytes_end, tx_bytes_end = get_all_netstats()
    read_bytes_end, write_bytes_end = get_all_diskstats()

    print "Finished running Tez query", query_num

    results = OrderedDict()
    results["run_time"] = end_time - start_time
    results["rx_bytes"] = rx_bytes_end - rx_bytes_start
    results["tx_bytes"] = tx_bytes_end - tx_bytes_start
    results["read_bytes"] = read_bytes_end - read_bytes_start
    results["write_bytes"] = write_bytes_end - write_bytes_start

    timeline, job_stats, job_timeline, hdfs_readers, non_aggregators, aggregators, hist_filename = get_all_tez_task_events()
    results["hdfs_readers"] = hdfs_readers
    results["aggregators"] = aggregators
    results["non_aggregators"] = non_aggregators
    results["hist_filename"] = hist_filename

    os.system("rm -rf output/history*")

    return results, timeline, job_stats, job_timeline

def write_mr_output(results, timeline, job_stats, graph, xml_graph=None):
    f = open(mr_output_file, 'a')
    f.write("%s\n" % results)

    for job_id, hdfs_bytes_read, hdfs_bytes_written, map_num, reduce_num in job_stats:
        f.write("%s %s %s %s %s\n" % (job_id, hdfs_bytes_read,
                                      hdfs_bytes_written, map_num, reduce_num))

    for key, val in graph.items():
        f.write("%s -> %s\n" % (key, val))

    f.write("\n")
    for t, task_event, task_type in timeline:
        f.write("%d %s %s\n" % (t, task_event, task_type))
    f.write("\n")

    f.write("-"*50)
    f.write("\n")
    f.close()

    if not xml_graph:
        return
    dot_filename = os.path.join(output_dir, "query-%s-xml.dot" %
                                results["query"])
    f = open(dot_filename, "w")
    dot = Digraph(comment="query-%s" % results["query"])
    for stage in xml_graph:
        dot.node(stage)
    for x, values in xml_graph.items():
        for y in values.keys():
            dot.edge(x, y)
    f.write("%s" % str(dot.source))
    f.close()

def write_mr_output_q3(results, timeline, job_stats, graph, filename):
    f = open(filename, 'a')
    f.write("%s\n" % results)

    for job_id, hdfs_bytes_read, hdfs_bytes_written, map_num, reduce_num in job_stats:
        f.write("%s %s %s %d %d\n" % (job_id, hdfs_bytes_read,
                                      hdfs_bytes_written, map_num, reduce_num))

    for key, val in graph.items():
        f.write("%s -> %s\n" % (key, val))

    f.write("\n")
    for t, task_event, task_type in timeline:
        f.write("%d %s %s\n" % (t, task_event, task_type))
    f.write("\n")

    f.write("-"*50)
    f.write("\n")
    f.close()


def write_tez_output(results, timeline, job_stats, job_timeline=None):
    f = open(tez_output_file, 'a')
    f.write("%s\n" % results)

    f.write("\n")
    for t, task_event, task_type in timeline:
        f.write("%d %s %s\n" % (t, task_event, task_type))
    f.write("\n")

    f.write("\n")
    for job_stat in job_stats:
        f.write("%s\n" % str(job_stat))
    f.write("\n")

    if job_timeline:
        f.write("\n")
        for job in job_timeline:
            f.write("%s\n" % str(job))
        f.write("\n")

    f.write("-"*50)
    f.write("\n")
    f.close()

def write_tez_output_q3(results, timeline, filename):
    f = open(filename, 'a')
    f.write("%s\n" % results)

    f.write("\n")
    for t, task_event, task_type in timeline:
        f.write("%d %s %s\n" % (t, task_event, task_type))
    f.write("\n")
    f.write("-"*50)
    f.write("\n")
    f.close()


def contains_tez_AM(output):
    lines = output.split("\n")
    for line in lines:
        count = line.count("DAGAppMaster")
        if count > 1:
            return True

    return False

def fail_tez_vm():
    for vm in slaves:
        output = check_output("ssh %s bash get_pid.sh DAGAppMaster" % vm)
        if not contains_tez_AM(output):
            datanode_pid = get_datanode_pid(output)
	    output = check_output("ssh %s bash get_pid.sh datanode" % vm)
            check_output("ssh %s kill %d" % (vm, datanode_pid))
            print ("Failed datanode on %s for Tez Query" % vm)
            return

def contains_mr_AM(output):
    lines = output.split("\n")
    for line in lines:
        count = line.count("MRAppMaster")
        if count > 1:
            return True

    return False

def fail_mr_vm():
    print "called fail mr vm function about to search for non master vm\n"*50
    for vm in slaves:
        output = check_output("ssh %s bash get_pid.sh MRAppMaster" % vm)
	print "got the output for ps..." 
        if not contains_mr_AM(output):
	    output = check_output("ssh %s bash get_pid.sh datanode" %vm)
            datanode_pid = get_datanode_pid(output)
	    print "found the pid for datanode..sending fail command" 
            check_output("ssh %s kill %d" % (vm, datanode_pid))
            print ("Failed datanode on %s for MR Query" % vm)
            return

def get_datanode_pid(output):
    lines = output.split("\n")
    for line in lines:
        count = line.count("datanode")
        if count > 1:
            line = line.split()
            pid = int(line[1])
            return pid
    raise Exception("Datanode PID not found")

def fail_tez_At(t):
    threading.Timer(t, fail_tez_vm).start()

def fail_mr_At(t):
    threading.Timer(t, fail_mr_vm).start()

def do_q3():
    
    query = 71
    restart_hadoop()    

    results, timeline, job_stats, graph, _ = run_mr_query(query)
    print results
    write_mr_output_q3(results, timeline, job_stats, graph, "final_q3_mr_base.txt")
    print "-" * 50
    print
    mr_base_time = results["run_time"]
        
    results, timeline, _, _ = run_tez_query(query)
    print results
    write_tez_output_q3(results, timeline, "final_q3_tez_base.txt")
    print "-" * 50
    print
    tez_base_time = results["run_time"]	
    
    restart_hadoop()       

    fail_mr_At(int(mr_base_time * 0.25))
    results, timeline, job_stats, graph, _ = run_mr_query(query)
    print results
    write_mr_output_q3(results, timeline, job_stats, graph, "final_mr_fail_25.txt")
    print "-" * 50

    restart_hadoop()       

    fail_mr_At(int(mr_base_time * 0.75))
    results, timeline, job_stats, graph, _ = run_mr_query(query)
    print results
    write_mr_output_q3(results, timeline, job_stats, graph, "final_mr_fail_75.txt")
    print "-" * 50
    
    restart_hadoop()       
    fail_tez_At(int(tez_base_time * 0.25))

    results, timeline, _, _ = run_tez_query(query)
    print results
    write_tez_output_q3(results, timeline, "final_tez_fail_25.txt")
    print "-" * 50
    print

    restart_hadoop()
    fail_tez_At(int(tez_base_time * 0.75))
    results, timeline, _, _ = run_tez_query(query)
    print results
    write_tez_output_q3(results, timeline, "final_tez_fail_75.txt")
    print "-" * 50
    print 

def restart_hadoop():
    subprocess.check_output( [ "bash", "-c", "source run.sh; stop_all" ] )
    subprocess.check_output( [ "bash", "-c", "source run.sh; start_all" ] )

def main():
    if os.path.exists(mr_output_file) or os.path.exists(tez_output_file):
        print "Please create a backup of previous output files and then remove them"
        sys.exit(1)

    for query in [12, 21, 50, 71, 85]:
        results, timeline, job_stats, graph, xml_graph = run_mr_query(query)
        print results
        write_mr_output(results, timeline, job_stats, graph, xml_graph)
        print "-" * 50
        print
        
        results, timeline, job_stats, job_timeline = run_tez_query(query)
        print results
        write_tez_output(results, timeline, job_stats, job_timeline)
        print "-" * 50
        print

if __name__ == '__main__':
    main()
