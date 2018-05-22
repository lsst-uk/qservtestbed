#!/usr/bin/env python

from __future__ import absolute_import, division, print_function

import logging
import shutil
import os
import sys

from lsst.qserv.admin import commons
import dataConfig
import qservDbLoader


class Benchmark(object):

    def __init__(self):

        self._dbName = "UKIDSS_Qservp3"
        self.config = commons.getConfig()
        self._out_dirname = os.path.join("/datapool/tmp/loader_tmp/", "test")
        self._in_dirname = os.path.join("/datapool/tmp/loader_test/", "test")
        self.dataReader = dataConfig.DataConfig(self._in_dirname)

    def loadData(self):
      for table in self.dataReader.orderedTables:
        self.dataLoader.createLoadTable(table)

    def cleanup(self):
        """
        Cleanup of previous tests temporary ant output files
        """
        if os.path.exists(self._out_dirname):
            shutil.rmtree(self._out_dirname)
        os.makedirs(self._out_dirname)

    def connectAndInitDatabases(self):
       self.dataLoader = qservDbLoader.QservLoader(
                self.config,
                self.dataReader,
                self._dbName,
                True,
                self._out_dirname
            )
       self.dataLoader.prepareDatabase()

    def finalize(self):
      self.dataLoader.workerInsertXrootdExportPath()
      # xrootd is restarted by wmgr

      # Reload Qserv (empty) chunk cache
      self.dataLoader.resetChunksCache()
      # Close socket connections
      del(self.dataLoader)

    def run(self):

      self.cleanup()
      print("1")
      self.connectAndInitDatabases()
      print("2")
      self.loadData()
      print("3")
      self.finalize()
      print("4")

if __name__ == "__main__":
  config = commons.read_user_config()
  bench = Benchmark()
  bench.run()
