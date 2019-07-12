#!/bin/bash

# Cores to turn ON
for id in {0..39};
#for id in 0 1 2 3 4 5 6 7 8 9 10 11
do
  echo -n "Core: $id - "
  echo 1 | sudo tee /sys/devices/system/cpu/cpu$id/online
  #echo ondemand | sudo tee /sys/devices/system/cpu/cpu$id/cpufreq/scaling_governor
  echo performance | sudo tee /sys/devices/system/cpu/cpu$id/cpufreq/scaling_governor
done

# Cores to turn OFF
#for id in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23
#for id in 12 13 14 15 16 17 18 19 20 21 22 23
#do
#  echo -n "Core: $id - "
#  echo powersave | sudo tee /sys/devices/system/cpu/cpu$id/cpufreq/scaling_governor
#  echo 0 | sudo tee /sys/devices/system/cpu/cpu$id/online
#done
