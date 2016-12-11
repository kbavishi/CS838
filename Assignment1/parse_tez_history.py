#! /usr/bin/python

import json
import os
import sys

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

def write_tez_output(results, timeline, job_stats, job_timeline=None):
    f = open("karan.txt", 'w')
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

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Please provide one argument: <tez history filepath>"
        sys.exit(1)
    timeline, job_stats, job_timeline, _, _, _ = get_tez_events(sys.argv[1])
    write_tez_output({}, timeline, job_stats, job_timeline)
