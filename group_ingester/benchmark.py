#
# LSST Data Management System
# Copyright 2008-2017 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""
Integration test tool :
- loads multiple datasets in Qserv

@author  Jacek Becla SLAC
@author  Fabrice Jammes IN2P3
"""

from __future__ import absolute_import, division, print_function

try:
    import configparser
except ImportError:
    import ConfigParser as configparser  # python2
import logging
import os
import shutil
import stat
import sys

from lsst.qserv.admin import commons
import dataConfig
import qservDbLoader

_LOG = logging.getLogger(__name__)

def is_multi_node():
    """ Check is Qserv install is multi node

        it assumes integration tests are launched
        on master for mono-node instance

        Returns
        -------

        true if Qserv install is multi-node
    """
    multi_node = True
    # FIXME code below is specific to mono-node setup
    # and might be removed
    config = commons.read_user_config()
    run_dir = config['qserv']['qserv_run_dir']
    config_file = os.path.join(run_dir, "qserv-meta.conf")
    if os.path.isfile(config_file):
        parser = configparser.SafeConfigParser()
        parser.read(config_file)
        if parser.get('qserv', 'node_type') in ['mono']:
            _LOG.info("Running Integration test in mono-node setup")
            multi_node = False
    return multi_node

class Benchmark(object):
    """Class implementing query running and result comparison for single test.

    Parameters
    ----------
    testdata_dir : str
        Location the directory containing test datasets
    out_dirname_prefix : str, optional
        Top-level directory for test outputs.
    """

    def __init__(self, testdata_dir, out_dirname_prefix, dbName):

        self.config = commons.read_user_config()

        self._multi_node = True

        if not out_dirname_prefix:
            out_dirname_prefix = self.config['qserv']['tmp_dir']
        self._out_dirname = out_dirname_prefix

        self._in_dirname = testdata_dir

        self.dataReader = dataConfig.DataConfig(self._in_dirname)
        self.dbName = dbName

    def loadData(self, dbName):
        dataLoader = self.connectAndInitDatabases(dbName)
        for table in self.dataReader.orderedTables:
            dataLoader.createLoadTable(table)
        dataLoader.finalize()

    def cleanup(self):
        """Cleanup of previous tests output files
        """
        if os.path.exists(self._out_dirname):
            shutil.rmtree(self._out_dirname)
        os.makedirs(self._out_dirname)

    def connectAndInitDatabases(self,dbName):
        """Establish database server connection and create database.

        Parameters
        ----------
        dbName : str
            Database name

        Returns
        -------
        `DbLoader` instance to be used for data loading
        """
        dataLoader = qservDbLoader.QservLoader(
            self.config,
            self.dataReader,
            dbName,
            self._multi_node,
            self._out_dirname
        )
        dataLoader.prepareDatabase()
        return dataLoader

    def run(self):

        self.cleanup()
        self.loadData(self.dbName)

