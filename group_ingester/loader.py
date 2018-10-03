#!/usr/bin/env python

from lsst.qserv.admin import commons
import benchmark
import os
import MySQLdb

def addChunk(dbName, tableName, chunkNum, workerName):

  nodes = os.popen('qserv-admin.py "SHOW NODES"').readlines()
  nodesInfo = {}
  success = False
  for node in nodes:
    info = node.split()
    if workerName == info[0]:
      host = info[2][5:]
      port = info[3][5:]
      active = info[4][7:]
      #FIXME Assume nodes use the same sock
      sock = config['mysqld']['socket']
      # create chunk table, chunk overlap table
      cmd = "mysql -D %s --host=%s --socket %s --user=%s --password=%s " % (dbName, host, sock, config['mysqld']['user'], config['mysqld']['pass'])
      dbcmd1 = '-e "create table %s_%d like %s_1234567890;"' % (tableName,chunkNum,tableName)
      dbcmd2 = '-e "create table %sFullOverlap_%d like %sFullOverlap_1234567890;"' % (tableName,chunkNum,tableName)
      print( cmd + dbcmd1 )
      ret1 = os.system(cmd + dbcmd1)
      print( cmd + dbcmd2 )
      ret2 = os.system(cmd + dbcmd2)
      if ret1 == 0 and ret2 == 0:
        success = True
      else:
        print("failed to create empty chunk")
        return
      cmd = "mysql -D %s --host=%s --socket %s --user=%s --password=%s " % ("qservw_worker", host, sock, config['mysqld']['user'], config['mysqld']['pass'])
      dbcmd = '-e "insert into Chunks (db,chunk) value(\'%s\', %d)"' % (dbName, chunkNum)
      print(cmd + dbcmd)
      ret = os.system(cmd + dbcmd)
      if ret != 0:
        print("failed to notify worker node")
  if not success:
    print("Unknown worker: " % workerName)

  # Register in css
  cssDB = MySQLdb.connect(host="localhost", user=config['mysqld']['user'], passwd=config['mysqld']['pass'], db="qservCssData", unix_socket=config['mysqld']['socket'])
  cursor = cssDB.cursor()
  cursor.execute('SELECT kvId FROM kvData WHERE kvKey="/DBS/%s/TABLES/%s/CHUNKS"' % (dbName, tableName))
  ChunkParentID = cursor.fetchone()[0]
  cursor.execute('SELECT max(kvID) FROM kvData')
  maxID = cursor.fetchone()[0]
  cursor.execute('INSERT INTO kvData (kvKey, kvVal, parentKvID) value("/DBS/%s/TABLES/%s/CHUNKS/%d","",%d)' % (dbName, tableName, chunkNum, ChunkParentID))
  cssDB.commit()
  cursor.execute('INSERT INTO kvData (kvKey, kvVal, parentKvID) value("/DBS/%s/TABLES/%s/CHUNKS/%d/REPLICAS", "", %d)' % (dbName, tableName, chunkNum, maxID + 1))
  cssDB.commit()
  cursor.execute('INSERT INTO kvData (kvKey, kvVal, parentKvID) value("/DBS/%s/TABLES/%s/CHUNKS/%d/REPLICAS/0000000001", "", %d)' % (dbName, tableName, chunkNum, maxID + 2))
  cssDB.commit()
  value = "\'{\"nodeName\":\"%s\"}\'" % workerName
  cursor.execute('INSERT INTO kvData (kvKey, kvVal, parentKvID) value("/DBS/%s/TABLES/%s/CHUNKS/%d/REPLICAS/0000000001/.packed.json",%s,%d)' % (dbName, tableName, chunkNum, value, maxID + 3))
  cssDB.commit()


if __name__ == '__main__':

  topDir = "/datapool/tmp/loader_test"
  groups = []
  WN = {}
  config = commons.read_user_config()

  for f in os.listdir(topDir):
    fn = os.path.join(topDir,f)
    if os.path.isdir(fn):
      # This is a group
      gmap = {"name": fn, "DBs": []}
      groups.append(gmap)
      gdir = os.path.join(topDir,fn)
      for f2 in os.listdir(gdir):
        if os.path.isdir(os.path.join(gdir,f2)):
          # This is a db
          DBDir = os.path.join(gdir,f2)
          DBFileList = os.listdir(DBDir)
          if not ("description.yaml" in DBFileList):
            continue
          DBMap = {"name" : f2, "dir" : DBDir, "tables": []}
          gmap["DBs"].append(DBMap)
          for f3 in DBFileList:
            if f3.endswith(".schema"):
              DBMap["tables"].append(f3[:-7]) 

  firstDB = None
  for group in groups:
    if os.path.exists("chunkMap.txt"):
      os.system("rm chunkMap.txt")
    if len(group["DBs"]) == 0: continue
    firstDB = group["DBs"][0]["name"]
    firstTable = group["DBs"][0]["tables"][0]

    first = True
    for db in group["DBs"]:
      data_dir = db["dir"]
      dbName = db["name"]
      temp_dir = os.path.join(data_dir, "temp")
      config = commons.read_user_config()
      bench = benchmark.Benchmark(data_dir, temp_dir, dbName)
      bench.run()

      # change css here
      if not first:
        cmd = "mysql -D qservCssData --port=%s --socket %s --user=%s --password=%s " % (config['mysqld']['port'], config['mysqld']['socket'], config['mysqld']['user'], config['mysqld']['pass'])
        # Change all other tables
        oldKey = "/DBS/%s/.packed.json" % dbName
        newKey = "/DBS/%s/.packed.json" % firstDB
        dbcmd = '-e "UPDATE qservCssData.kvData css1, qservCssData.kvData css2 SET css1.kvVal=css2.kvVal WHERE css1.kvKey=\'%s\' AND css2.kvVal=\'%s\';"' % (oldKey, newKey)
        print(cmd + dbcmd)
        os.system(cmd + dbcmd)
      first = False

      for table in db["tables"]:
        currentChunk = []
        out = os.popen('qserv-admin.py "SHOW CHUNKS %s.%s"' % (dbName, table)).readlines()
        for item in out:
          if item.startswith("chunk"):
            currentChunk.append(int(item.split(":")[1]))

        toAdd = {}
        allChunk = {}
        chunkF = open("chunkMap.txt",'r')
        chunkList = chunkF.readlines()
        for chunk in chunkList:
          allChunk[int(chunk.split()[0])] = chunk.split()[1]
        chunkF.close()

        for cn in allChunk.keys():
          if cn not in currentChunk: toAdd[cn] = allChunk[cn]
        for chunkNum, worker in toAdd.items():
          addChunk(dbName, table, chunkNum, worker)
