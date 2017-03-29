#!/bin/bash


echo 'START config Sharding'

echo 'Make directories for db data in /data/...'
#make directories
mkdir -p /data/logs
mkdir -p /data/shard1_node0
mkdir -p /data/shard1_node1
mkdir -p /data/shard1_node2
mkdir -p /data/shard2_node0
mkdir -p /data/shard2_node1
mkdir -p /data/shard2_node2
mkdir -p /data/shard3_node0
mkdir -p /data/shard3_node1
mkdir -p /data/shard3_node2
mkdir -p /data/cfg_node0
mkdir -p /data/cfg_node1
mkdir -p /data/cfg_node2
echo 'Make directories for db data...OK'

#Config servers
echo 'Starting up config servers (27000, 27001, 27002)...'
mongod --configsvr --replSet "cfg" --port 27000 --logpath /data/logs/log.cfg_node0 --logappend --dbpath /data/cfg_node0 --fork
mongod --configsvr --replSet "cfg" --port 27001 --logpath /data/logs/log.cfg_node1 --logappend --dbpath /data/cfg_node1 --fork
mongod --configsvr --replSet "cfg" --port 27002 --logpath /data/logs/log.cfg_node2 --logappend --dbpath /data/cfg_node2 --fork
echo 'Starting up config servers...OK'
#Replica Sets -> Shards
echo 'Starting up shard1 (27010, 27011, 27012)...'
mongod --shardsvr --replSet "shard1" --dbpath /data/shard1_node0 --logpath /data/logs/log.shard1_node0 --port 27010 --fork --logappend --smallfiles --oplogSize 50
mongod --shardsvr --replSet "shard1" --dbpath /data/shard1_node1 --logpath /data/logs/log.shard1_node1 --port 27011 --fork --logappend --smallfiles --oplogSize 50
mongod --shardsvr --replSet "shard1" --dbpath /data/shard1_node2 --logpath /data/logs/log.shard1_node2 --port 27012 --fork --logappend --smallfiles --oplogSize 50
echo 'Starting up shard1...OK'

echo 'Starting up shard2 (27020, 27021, 27022)...'
mongod --shardsvr --replSet "shard2" --dbpath /data/shard2_node0 --logpath /data/logs/log.shard2_node0 --port 27020 --fork --logappend --smallfiles --oplogSize 50
mongod --shardsvr --replSet "shard2" --dbpath /data/shard2_node1 --logpath /data/logs/log.shard2_node1 --port 27021 --fork --logappend --smallfiles --oplogSize 50
mongod --shardsvr --replSet "shard2" --dbpath /data/shard2_node2 --logpath /data/logs/log.shard2_node2 --port 27022 --fork --logappend --smallfiles --oplogSize 50
echo 'Starting up shard2...OK'

echo 'Starting up shard3 (27030, 27031, 27032)...'
mongod --shardsvr --replSet "shard3" --dbpath /data/shard3_node0 --logpath /data/logs/log.shard3_node0 --port 27030 --fork --logappend --smallfiles --oplogSize 50
mongod --shardsvr --replSet "shard3" --dbpath /data/shard3_node1 --logpath /data/logs/log.shard3_node1 --port 27031 --fork --logappend --smallfiles --oplogSize 50
mongod --shardsvr --replSet "shard3" --dbpath /data/shard3_node2 --logpath /data/logs/log.shard3_node2 --port 27032 --fork --logappend --smallfiles --oplogSize 50
echo 'Starting up shard3...OK'

#27000
echo 'Initiate rs cfg'
mongo -port 27000 --eval '
rs.initiate(
   {
      _id: "cfg",
      version: 1,
      members: [
         { _id: 0, host : "127.0.0.1:27000" },
         { _id: 1, host : "127.0.0.1:27001" },
         { _id: 2, host : "127.0.0.1:27002" }
      ]
   }
);'

# 27010
echo 'Initiate shard1'
mongo -port 27010 --eval '
rs.initiate(
   {
      _id: "shard1",
      version: 1,
      members: [
         { _id: 0, host : "127.0.0.1:27010" },
         { _id: 1, host : "127.0.0.1:27011" },
         { _id: 2, host : "127.0.0.1:27012" }
      ]
   }
)'

# 27020
echo 'Initiate shard2'
mongo -port 27020 --eval '
rs.initiate(
   {
      _id: "shard2",
      version: 1,
      members: [
         { _id: 0, host : "127.0.0.1:27020" },
         { _id: 1, host : "127.0.0.1:27021" },
         { _id: 2, host : "127.0.0.1:27022" }
      ]
   }
)'

#27030
echo 'Initiate shard3'
mongo -port 27030 --eval '
rs.initiate(
   {
      _id: "shard3",
      version: 1,
      members: [
         { _id: 0, host : "127.0.0.1:27030" },
         { _id: 1, host : "127.0.0.1:27031" },
         { _id: 2, host : "127.0.0.1:27032" }
      ]
   }
)'


#echo 'Sleep 30 sg, waiting initialize of rs'
#sleep 30

#Router
echo 'Starting up mongos (27017)'
mongos --configdb cfg/127.0.0.1:27000,127.0.0.1:27001,127.0.0.1:27002 --fork --logappend --logpath /data/logs/log.mongos0
echo 'Starting up mongos OK'

echo 'Adding shards to mongos'
mongo --eval 'sh.addShard("shard1/127.0.0.1:27010")';
mongo --eval 'sh.addShard("shard2/127.0.0.1:27020")';
mongo --eval 'sh.addShard("shard3/127.0.0.1:27030")';


#function killshardnode {
#
#  if [ ! -z "$1" ] && [ ! -z "$2" ] && [ "$1" -ge 0 ] && [ "$2" -ge 0 ]; then
#    kill `cat /data/shard$1_node$2/mongod.lock`
#    if [ $? -eq 0 ]; then
#      echo "OK shard $1 node $2 killed"
#    else
#      echo "ERROR killing shard $1 node $2"
#    fi
#  else
#    echo "bad parameters, expected: killshardnode 1 1"
#  fi;
#};
#
#export -f killshardnode;
#
#function killcfgnode {
#  if [ ! -z "$1" ] && [ "$1" -ge 0 ]; then
#    kill `cat /data/cfg_node$1/mongod.lock`
#    if [ $? -eq 0 ]; then
#      echo "OK cfg shard $1 killed"
#    else
#      echo "ERROR killing cfg shard $1"
#    fi
#  else
#    echo "bad parameters, expected: killcfgnode 1"
#  fi;
#};
#
#export -f killcfgnode;
#
echo "MongoDB Sharding config finish"

while true; do
  sleep 30;
done

# CONNECT TO MONGOS

# change the chunk size, between 1 and 1024 megabytes
# use config
# db.settings.save( { _id:"chunksize", value: <sizeInMB> } )

# Enable sharding for each database:
# sh.enableSharding("database_name");

# Enable sharding for each collection:
# db.dataset.createIndex({"restaurant_id":1})
# sh.shardCollection("test.dataset", {"restaurant_id":1})
# sh.shardCollection("test.delicious", {"_id":1})
