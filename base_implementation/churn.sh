#!/bin/bash - 

for i in {1..50}
do
   echo "Killing node $i"
   pkill -n lua
   sleep 10
done
