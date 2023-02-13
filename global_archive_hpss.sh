#!/bin/bash -l
#
# global RTOFS data pull from HPSS
#

module load hpss/hpss

TODAY=$(date "+%Y%m%d")
YESTERDAY=$(date --date='yesterday' "+%Y%m%d")
RUNDATE=${1:-$YESTERDAY}

echo "Rundate: $RUNDATE" 

WORKDIR=/scratch2/NCEPDEV/stmp1/Deanna.Spindler/cdo/$RUNDATE
ARCHDIR=/scratch2/NCEPDEV/ocean/Deanna.Spindler/noscrub/Global/archive
SRCDIR=/scratch2/NCEPDEV/ocean/Deanna.Spindler/save/VPPPG/Global_RTOFS/EMC_ocean-prod-gen/dataproc/scripts

mkdir -p $ARCHDIR

$SRCDIR/hpss_extractor.sh $RUNDATE

# move datasets to archive
cp -r $WORKDIR $ARCHDIR/.
rm -rf $WORKDIR

exit
