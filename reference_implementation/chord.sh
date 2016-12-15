#!/bin/bash

max=$1
if [[ $max == "" ]]
then
  max=64
fi

killall lua
sleep 1



for (( n=1;n<=$max;n++ ))
do
  rm $n.log > /dev/null 2>&1
  #lua chord.lua $n $max > logs/$n.log 2>&1 &
  lua chord.lua $n $max &
done
