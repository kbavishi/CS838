#!/usr/bin/python

import sys

rack1 = "/10.10.1.0"
rack2 = "GD_RACK"

costs = {
    (rack1, rack1): 0,
    (rack1, rack2): 100,
    (rack2, rack1): 100,
    (rack2, rack2): 0,
}

key = tuple(sys.argv[1:3])
print costs.get(key, 1)
