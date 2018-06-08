#!/usr/bin/env python

from __future__ import absolute_import, division, print_function

# ----------------------------
# Imports for other modules --
# ----------------------------
from lsst.qserv.admin import commons
import benchmark

if __name__ == '__main__':

    testdata_dir = '/datapool/tmp/loader_test/test'
    out_dir = '/datapool/tmp/loader_tmp/test'
    config = commons.read_user_config()
    bench = benchmark.Benchmark(testdata_dir, out_dir)
    bench.run()
