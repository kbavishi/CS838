import sys
import hadoop_testlib

if __name__ == '__main__':
    if len(sys.argv) != 6:
        print "Please provide all 5 VM hostname:port values"
        sys.exit(1)

    nn, rm, slave0, slave1, slave2 = sys.argv[1:]
    nn_shell = hadoop_testlib.setup_hadoop_testbase(nn, rm, slave0, slave1, slave2)

    nn_shell.run_hadoop_cmd("start_all")
    output = ""
    output += hadoop_testlib.run_TestDFSIO(nn_shell, test_type="write")
    output += hadoop_testlib.run_TestDFSIO(nn_shell, test_type="read")
    nn_shell.run_hadoop_cmd("stop_all")

    hadoop_testlib.save_output(output, "simple_test.txt")
