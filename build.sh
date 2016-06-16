#!/bin/bash
if [ "$#" -eq 1 ]; then
	set -x
fi

echo "preparing the archive"

GOOS=linux go build -v github.com/gevgev/numerxdatapusher
rc=$?; if [[ $rc != 0 ]]; then 
	echo "sqlpusher build failed"
	exit $rc; 
fi

zip -r archive.zip numerxdatapusher
rc=$?; if [[ $rc != 0 ]]; then 
	echo "could not create the archive file"
	exit $rc; 
fi

echo "Success"
