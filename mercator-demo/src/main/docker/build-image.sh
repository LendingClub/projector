#!/bin/bash


#NEO4J_VERSION-3.2.3

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_DIR=$SCRIPT_DIR/../../..

cd $SCRIPT_DIR

JAR_FILE=$(find ${SRC_DIR}/build/libs -name '*-all.jar' | head -1 )

rm -rf mercator/lib
mkdir -p mercator/lib

docker info
if [ ! $? -eq 0 ]; then
    echo ERROR: docker not operational
    exit 0
fi

cp -v ${JAR_FILE} ${SCRIPT_DIR}/mercator/lib/mercator-demo.jar || exit 99


docker build . -t mercator-demo || exit 99

docker tag mercator-demo:latest lendingclub/mercator-demo || exit 99