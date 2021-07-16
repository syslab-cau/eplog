#!/bin/bash

devsize=0
blockdevsize=0

for i in b; do
	devname="/dev/sd${i}"
	blockdevsize=`sudo blockdev --getsz $devname`
	devsize=`echo "$devsize + $blockdevsize" | bc`
	echo "$devname $blockdevsize $devsize"
	devlist="$devlist$devname "
	let DEVS="$DEVS+1"
done

devname="/dev/nvme0n1"
blockdevsize=`sudo blockdev --getsz $devname`
devsize=`echo "$devsize + $blockdevsize" | bc`
echo "$devname $blockdevsize $devsize"
devlist="$devlist$devname "
let DEVS="$DEVS+1"
