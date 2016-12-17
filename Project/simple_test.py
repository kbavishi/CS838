import sys
import hadoop_testlib

if __name__ == '__main__':
    if len(sys.argv) != 6:
        print "Please provide all 5 VM hostname:port values"
        sys.exit(1)

    nn, rm, slave0, slave1, slave2 = sys.argv[1:]
    slaves = [slave0, slave1, slave2]
    nn_shell, _, slave_shells = \
        hadoop_testlib.setup_hadoop_testbase(nn, rm, slaves)

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
        output += hadoop_testlib.run_TestDFSIO(nn_shell, slave_shells,
                                               test_type="write",
                                               number_of_files=1,
                                               file_size='64MB')
        output += hadoop_testlib.run_TestDFSIO(nn_shell, slave_shells,
                                               test_type="read",
                                               number_of_files=1,
                                               file_size='64MB')

        # Tests for big files
        output += hadoop_testlib.run_TestDFSIO(nn_shell, slave_shells,
                                               test_type="write",
                                               number_of_files=1,
                                               file_size='1GB')
        output += hadoop_testlib.run_TestDFSIO(nn_shell, slave_shells,
                                               test_type="read",
                                               number_of_files=1,
                                               file_size='1GB')

        # After each iteration, just save the results
        hadoop_testlib.save_output(output, "hdfs_replica_3.txt")

    hadoop_testlib.cleanup_TestDFSIO(nn_shell)
    hadoop_testlib.stop_all(nn_shell)

    hadoop_testlib.save_output(output, "hdfs_replica_3.txt")
