#!/bin/bash


/mercator/start-neo4j.sh

cd /mercator

JAR_FILE=$(find ./lib -name 'mercator-demo.jar' | head -1)

exec java -jar ${JAR_FILE} 


