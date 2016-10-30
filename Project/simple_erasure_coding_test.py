#! /usr/bin/python

import sys
import hadoop_testlib

if __name__ == '__main__':
    if len(sys.argv) != 8:
        print "Please provide all 7 VM hostname:port values"
        sys.exit(1)

    # Q. Why do we need 5 slave nodes you ask?
    # A. For an erasure coding scheme (R,S), we need R+S slave nodes to store
    # the striped blocks. Without this, HDFS complains
    nn, rm, slave0, slave1, slave2, slave3, slave4 = sys.argv[1:]
    slaves = [slave0, slave1, slave2, slave3, slave4]
    nn_shell = hadoop_testlib.setup_hadoop_testbase(nn, rm, slaves)

    nn_shell.run_hadoop_cmd("start_all")
    nn_shell.run_hadoop_cmd("hadoop fs -mkdir -p /benchmarks")
    # The output of TestDFSIO is written to /benchmarks in HDFS. Change the
    # storage policy for that directory. The available policies are:
    # 1. RS-DEFAULT-3-2-64k
    # 2. RS-DEFAULT-6-3-64k
    # 3. RS-LEGACY-6-3-64k
    policy =  nn_shell.run_hadoop_cmd("hdfs erasurecode -getPolicy /benchmarks").output
    if "RS" not in policy:
        ec_cmd = "hdfs erasurecode -setPolicy -p RS-DEFAULT-3-2-64k /benchmarks"
        nn_shell.run_hadoop_cmd(ec_cmd)

    # Run our TestDFSIO tests. Run at least 10 times to average out any noise in
    # measurements
    output = ""
    for _ in xrange(10):
        # Run tests for both large and small files.
        # According to the HDFS scalability paper by Shvachko, the average file
        # size in Yahoo!  clusters is 1.5 blocks. So for our definition of
        # "small files", we pick a size of 1 block ie. 64 MB. Whereas for large
        # files, we pick a size of 1 GB

        # Tests for small files
        output += hadoop_testlib.run_TestDFSIO(nn_shell, test_type="write",
                                               number_of_files=1,
                                               file_size='64MB')
        output += hadoop_testlib.run_TestDFSIO(nn_shell, test_type="read",
                                               number_of_files=1,
                                               file_size='64MB')

        # Tests for big files
        output += hadoop_testlib.run_TestDFSIO(nn_shell, test_type="write",
                                               number_of_files=1,
                                               file_size='1GB')
        output += hadoop_testlib.run_TestDFSIO(nn_shell, test_type="read",
                                               number_of_files=1,
                                               file_size='1GB')

    nn_shell.run_hadoop_cmd("stop_all")
    # XXX Seems like you can't unset a policy back to cold replicas

    # Dump the output to a txt file so that it can be useful later
    hadoop_testlib.save_output(output, "erasure_coding_RS_3_2.txt")
