#! /usr/bin/python

import os
import math
import csv

mr_timeline_dir = "mr-timeline/"
tez_timeline_dir = "tez-timeline/"

def parse_and_adjust(query, timeline_dir):
    #filename = os.path.join(timeline_dir, "query-%d.txt" % query)
    filename = os.path.join(timeline_dir, "query-%d-q3.txt" % query)
    print filename
    lines = open(filename, "r").readlines()
    if not lines:
        print "no lines found"
        return
    start_time = int(lines[0].split()[0])
    timeline = {}
    total_non_agg, total_agg = 0, 0

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if "HDFS" in line:
            continue
        t, event_type, task_type = line.split()
        t = int(t)
        time = (t - start_time) * 1./1000
        if task_type in ["MAP", "NON_AGGREGATOR"]:
            if event_type == "TASK_STARTED":
                total_non_agg += 1
            else:
                total_non_agg -= 1
        elif task_type in ["REDUCE", "AGGREGATOR"]:
            if event_type == "TASK_STARTED":
                total_agg += 1
            else:
                total_agg -= 1
        adj_time = int(math.ceil(time))
        timeline[adj_time] = (total_non_agg, total_agg)

    csv_path = os.path.join(timeline_dir, "timeline-%d.csv" % query)
    print csv_path
    csvfile = open(csv_path, 'wb')
    writer = csv.writer( csvfile )
    prev_t, prev_t_nagg, prev_t_agg = 0, 0, 0
    for t in xrange(0, adj_time+1):
        if t in timeline:
            writer.writerow([t, timeline[t][0], timeline[t][1]])
            prev_t_nagg, prev_t_agg = timeline[t][0], timeline[t][1]
        else:
            # Use previously remembered values
            writer.writerow([t, prev_t_nagg, prev_t_agg])

if __name__ == '__main__':
    parse_and_adjust(71, mr_timeline_dir)
    parse_and_adjust(71, tez_timeline_dir)
    #for query in [12, 21, 50, 71, 85]:
    #    parse_and_adjust(query, mr_timeline_dir)
    #    parse_and_adjust(query, tez_timeline_dir)
