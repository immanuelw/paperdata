#!/bin/sh
for i in folio node01 node02 node03 node04 node05 node06 node07 node08 node09 node10; do ssh $i python /data2/home/immwa/scripts/paper/paperdataDB/folio/node_log.py & done
