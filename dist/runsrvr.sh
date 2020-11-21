#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

# Include your ZooKeeper connection string here. Make sure there are no spaces.
#	Replace with your server names and client ports.
export ZKSERVER=lab19.cs.mcgill.ca:21825,lab20.cs.mcgill.ca:21825,lab21.cs.mcgill.ca:21825

java -cp $CLASSPATH:../task:.: DistProcess 
