#!/bin/bash

ret=0

all_containers=$(ansible all -a "ctr -n k8s.io c ls -q" --become | grep "^[0-9a-f].*$")

found=0
for container in `kubectl get pod $1 $2 -ojsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.containerID}{"\n"}{end}' | cut -d/ -f3`; do
	echo $all_containers | grep -q $container
	if [ $? == 0 ]; then
		found=1
		ret=1
		echo "Found container $container of pod $1 still running"
	fi
done

if [ $found == 0 ]; then
	kubectl delete pod $1 $2 --force --grace-period=0
fi

exit $ret
