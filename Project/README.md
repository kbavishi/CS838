# How to write simple Hadoop tests

You can write your tests using the library functions provided in `hadoop_testlib.py`. You should find a lot of helpful references in `simple_test.py`. 

# How to run those Hadoop tests

In most cases, you'll need to provide arguments to your test (Eg. `simple_test.py`) in the form `hostname:ssh_port`. The hostname and SSH port can be found from the *List View* in the CloudLab experiment page. 

For example, you could run the `simple_test.py` in the following manner

```bash
python simple_test.py c220g1-031109:30522 c220g1-031109:30523 c220g1-031109:30524 c220g1-031109:30525 c220g1-031109:30526
``` 

## NOTE

Note that the test and test library may assume that the hostnames and ports are provided in a certain order. Eg. `simple_test.py` assumes that arguments are provided in the following order:
* Namenode
* ResourceManager
* Slave0
* Slave1
* Slave2
