#!/bin/bash -l

when=$1

NEWHOME=/scratch2/NCEPDEV/ocean/Deanna.Spindler
SRCDIR=${NEWHOME}/save/VPPPG/Global_RTOFS

# GODAE Dataset generation and upload
${SRCDIR}/EMC_ocean-verification/godae/scripts/godae.sh $when 1>${NEWHOME}/save/Logs/godae.log  2>&1 
