import sys
import hadoop_testlib

# XXX kbavishi - Extra things I had to do get this to work:
# 1. ifconfig eth1 down on GDA slave
# 2. Fix hostnames on both non-GDA slaves. This was causing the RM to connect to
# weird hostnames
if __name__ == '__main__':
    if len(sys.argv) != 6:
        print "Please provide all 5 VM hostname:port values"
        sys.exit(1)

    nn, rm, slave0, slave1, slave2 = sys.argv[1:]
    slaves = [slave0, slave1, slave2]
    nn_shell, _, slave_shells = \
        hadoop_testlib.setup_hadoop_testbase(nn, rm, slaves,
                                             allow_public_ip=True)

    hadoop_testlib.start_all(nn_shell)

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

        # After each iteration, just save the results
        hadoop_testlib.save_output(output, "gda_hdfs_replica_3.txt")

    hadoop_testlib.cleanup_TestDFSIO(nn_shell)
    hadoop_testlib.stop_all(nn_shell)

    hadoop_testlib.save_output(output, "gda_hdfs_replica_3.txt")
