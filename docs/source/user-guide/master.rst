Resource Manager (Master)
============================

The cluster contains dataNodes,metaNodes,vols,dataPartitions and metaPartitions,they are managed by master server. The master server caches the metadata in mem,persist to GoLevelDB,and ensure consistence by raft protocol.

The master server manages dataPartition id to dataNode server mapping,metaPartition id to metaNode server mapping.

At lease 3 master nodes are required in respect to high availability.

Features
--------

- Multi-tenant, Resource Isolation
- dataNodes,metaNodes shared,vol owns dataPartition and metaPartition exclusive
- Asynchronous communication with dataNode and metaNode

Configurations
--------------

ChubaoFS use **JSON** as configuration file format.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"
   
   "role", "string", "Role of process and must be set to master", "Yes"
   "ip", "string", "host ip", "Yes"
   "listen", "string", "Http port which api service listen on", "Yes"
   "prof", "string", "golang pprof port", "Yes"
   "id", "string", "identy different master node", "Yes"
   "peers", "string", "the member information of raft group", "Yes"
   "logDir", "string", "Path for log file storage", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*.", "No"
   "retainLogs", "string", "the number of raft logs will be retain.", "Yes"
   "walDir", "string", "Path for raft log file storage.", "Yes"
   "storeDir", "string", "Path for RocksDB file storage,path must be exist", "Yes"
   "clusterName", "string", "The cluster identifier", "Yes"
   "exporterPort", "int", "The prometheus exporter port", "No"
   "consulAddr", "string", "The consul register addr for prometheus exporter", "No"
   "metaNodeReservedMem","string","If the metanode memory is below this value, it will be marked as read-only. Unit: byte. 1073741824 by default.", "No"


**Example:**

.. code-block:: json

   {
    "role": "master",
    "id":"1",
    "ip": "10.196.59.198",
    "listen": "17010",
    "prof":"17020",
    "peers": "1:10.196.59.198:17010,2:10.196.59.199:17010,3:10.196.59.200:17010",
    "retainLogs":"20000",
    "logDir": "/cfs/master/log",
    "logLevel":"info",
    "walDir":"/cfs/master/data/wal",
    "storeDir":"/cfs/master/data/store",
    "exporterPort": 9500,
    "consulAddr": "http://consul.prometheus-cfs.local",
    "clusterName":"chubaofs01",
    "metaNodeReservedMem": "1073741824"
   }


Start Service
-------------

.. code-block:: bash

   nohup ./master -c config.json > nohup.out &
