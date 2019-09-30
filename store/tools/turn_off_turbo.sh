#!/bin/bash
#https://wiki.archlinux.org/index.php/CPU_frequency_scaling
echo 1 > sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
#echo 0 > sudo tee /sys/devices/system/cpu/cpufreq/boost
