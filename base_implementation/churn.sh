#!/bin/bash - 

for i in {1..100}
do
   echo "Killing node $i"
   pkill -n lua
   sleep 5
done
