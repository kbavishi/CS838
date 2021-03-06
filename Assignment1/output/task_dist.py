#!usr/bin/python

from collections import OrderedDict
import sys
file_name = "tasks_dist.txt"


def main():
    q_num = sys.argv[1]
    f = open(file_name)
    lines = f.readlines()
    num_tasks, start_time = 0, 0
    read_start_time = False
    mr = OrderedDict() 
    tez = OrderedDict()
    for l in lines:

        if "MR" in l or "TEZ" in l:
            read_start_time = True
            continue

        if read_start_time == True:
            start_time = int(l.split()[0])
            read_start_time = False

        if "TASK_STARTED" in l:
            num_tasks += 1
        elif "TASK_FINISHED" in l or "TASK_FAILED" in l:
            num_tasks -=1

        cur_time = int(l.split()[0]) - start_time
        
        if "MAP" in l or "REDUCE" in l:
           mr[cur_time/1000] = num_tasks
        else:
           tez[cur_time/1000] = num_tasks

    print "done"
    #print mr
    #print tez
    outfile  = "dist_mr_" +  q_num + ".csv"
    out = open(outfile, "w")
    for time, tasks in mr.items():
        out.write("%s,%s\n"% (time, tasks))



    outfile  = "dist_tez_" +  q_num + ".csv"
    out = open(outfile, "w")
    for time, tasks in mr.items():
        out.write("%s,%s\n"% (time, tasks))
    out.close()
if __name__ == "__main__":
    main()
