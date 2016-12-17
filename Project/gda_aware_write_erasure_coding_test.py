#! /usr/bin/python

import sys
import hadoop_testlib

if __name__ == '__main__':
    if len(sys.argv) != 9:
        print "Please provide all 8 VM hostname:port values"
        sys.exit(1)

    # 5 slaves in the same DC. 1 slave is in another DC
    nn, rm, slave0, slave1, slave2, slave3, slave4, slave5 = sys.argv[1:]
    slaves = [slave0, slave1, slave2, slave3, slave4, slave5]
    nn_shell, _, slave_shells = \
        hadoop_testlib.setup_hadoop_testbase(nn, rm, slaves,
                                             allow_public_ip=True,
                                             link_awareness=False)

    hadoop_testlib.start_all(nn_shell)

    # The output of TestDFSIO is written to /benchmarks in HDFS. Change the
    # storage policy for that directory. 
    path = "/benchmarks"
    nn_shell.run_hadoop_cmd("hadoop fs -mkdir -p %s" % path)
    # We pick RS-DEFAULT-3-2-64k for now because it uses the least number of
    # slave nodes.
    hadoop_testlib.set_ec_policy(nn_shell, path, "RS-DEFAULT-3-2-64k")

    # Run our TestDFSIO tests. Run at least 10 times to average out any noise in
    # measurements
    output = ""
    for _ in xrange(10):
        # Tests for big files
        output += hadoop_testlib.run_TestDFSIO(nn_shell, slave_shells,
                                               test_type="write",
                                               number_of_files=1,
                                               file_size='1GB')
        filepath = "/benchmarks/TestDFSIO/io_data/test_io_0"
        hadoop_testlib.verify_ec_policy(nn_shell, filepath, 3, 2)

        # After each iteration, just save the results
        hadoop_testlib.save_output(output,
                                   "gda_aware_write_erasure_coding_RS_3_2.txt")

    hadoop_testlib.cleanup_TestDFSIO(nn_shell)
    hadoop_testlib.stop_all(nn_shell)
    # XXX Seems like you can't unset a policy back to cold replicas

    # Dump the output to a txt file so that it can be useful later
    hadoop_testlib.save_output(output, "gda_aware_write_erasure_coding_RS_3_2.txt")
