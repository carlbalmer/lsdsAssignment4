#!/bin/bash - 
#===============================================================================
#
#          FILE: launch-daemons.sh
# 
#         USAGE: ./launch-daemons.sh 
# 
#   DESCRIPTION: Allows launching N instances of the program: skeleton_anti_entropy.lua
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Raziel Carvajal-Gomez (RCG), raziel.carvajal@unine.ch
#  ORGANIZATION: 
#       CREATED: 10/06/2016 13:14
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error
daemons=$1

rm -fr logs
touch logs
for (( CNTR=1;CNTR<$daemons+1; CNTR+=1 )); do
  echo "Launching daemon number: $CNTR"
  lua t_chord.lua $CNTR $daemons &>>logs &
  echo "DONE"
done
