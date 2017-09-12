#!/bin/bash



setting() {
    setting="${1}"
    value="${2}"
    file="/var/lib/neo4j/conf/neo4j.conf"

    if [ -n "${value}" ]; then
        if grep -q -F "${setting}=" ${file} ; then
            sed --in-place "s|.*${setting}=.*|${setting}=${value}|" "${file}"
        else
            echo "${setting}=${value}" >>"${file}"
        fi
    fi
}


setting "dbms.security.auth_enabled" "false"
setting "dbms.tx_log.rotation.retention_policy" "${NEO4J_dbms_txLog_rotation_retentionPolicy:-100M size}"
setting "dbms.memory.pagecache.size" "${NEO4J_dbms_memory_pagecache_size:-512M}"
setting "wrapper.java.additional=-Dneo4j.ext.udc.source" "${NEO4J_UDC_SOURCE:-docker}"
setting "dbms.memory.heap.initial_size" "${NEO4J_dbms_memory_heap_maxSize:-512M}"
setting "dbms.memory.heap.max_size" "${NEO4J_dbms_memory_heap_maxSize:-512M}"
setting "dbms.unmanaged_extension_classes" "${NEO4J_dbms_unmanagedExtensionClasses:-}"
setting "dbms.allow_format_migration" "${NEO4J_dbms_allowFormatMigration:-}"


setting "dbms.connectors.default_listen_address" "0.0.0.0"
setting "dbms.connector.http.listen_address" "0.0.0.0:7474"
setting "dbms.connector.https.listen_address" "0.0.0.0:7473"
setting "dbms.connector.bolt.listen_address" "0.0.0.0:7687"

cd /var/lib/neo4j/bin

./neo4j start


NEO4J_STARTED="0"

while [ "${NEO4J_STARTED}" -eq "0" ]; do
  
  NEO4J_STARTED=$(curl -s http://localhost:7474 | grep data | wc -l)
  
  echo waiting for neo4j to start...
  sleep 5
  
done


