#!/bin/bash
#
# Copy Global RTOFS and GEFS-Wave grib2 files to Hera
#

# set up module environment
module reset
module load intel/19.1.3.304
module load netcdf/4.7.4

TODAY=`date "+%Y%m%d"`
YESTERDAY=`date --date='yesterday' "+%Y%m%d"`
RUNDATE=${2:-$TODAY}
GODAE_DATE=`date --date="${RUNDATE} -6days" "+%Y%m%d"`
PDYm2=`date --date="${RUNDATE} -2days" "+%Y%m%d"`

STAGE='wcoss2hera'

echo "relay $STAGE started at `date`"

echo "Rundate: $RUNDATE" 

NEWHOME=/lfs/h2/emc/vpppg/noscrub/deanna.spindler
COM=/lfs/h1/ops/prod/com/rtofs/v2.3
GEFS=/lfs/h1/ops/prod/com/gefs/v12.3
DATAURL=$COM/rtofs.${YESTERDAY}
REMOTE_IN=/scratch2/NCEPDEV/ocean/Deanna.Spindler/noscrub
REMOTE_OUT=/scratch2/NCEPDEV/stmp1/Deanna.Spindler
TRANSFER=/lfs/h2/emc/ptmp/deanna.spindler/hera_transfer
ARCDIR=$TRANSFER/incoming/Global/archive/${YESTERDAY}

if [[ $RUNDATE = $TODAY ]]; then
  GEFS=$GEFS/gefs.${PDYm2}
  GEFS_ARC=$TRANSFER/incoming/GEFS_grib/archive/${PDYm2}
else
  GEFS=$GEFS/gefs.${RUNDATE}
  GEFS_ARC=$TRANSFER/incoming/GEFS_grib/archive/${RUNDATE}
fi
GODAE_HERA=/scratch2/NCEPDEV/ocean/Deanna.Spindler/noscrub/GODAE
RUSER=Deanna.Spindler@dtn-hera.fairmont.rdhpcs.noaa.gov
POLAR=emc.rtofs@emcrzdm:/home/www/polar
POLAR_WAVES=waves@emcrzdm:/home/www/polar

RSYNC_OPTS='-av --timeout=300'

# WCOSS2HERA

if [[ $STAGE = "wcoss2hera" ]]; then
  
  mkdir -p $ARCDIR
  
  # get latest RTOFS data for GODAE
  cd $ARCDIR  
  fcsts='n024 f024 f048 f072 f096 f120 f144 f168 f192'
  for fcst in ${fcsts}; do
    pattern="rtofs_glo_*_${fcst}_*.nc"
    cp -p --no-clobber ${DATAURL}/${pattern} . &
  done
  wait  
  rm -f *hvr*.nc  # don't need these
  
  # get latest GEFS-Wave data
  mkdir -p $GEFS_ARC
  cd $GEFS_ARC
  fcsts='f000 f024 f048 f072 f096 f120 f144 f168 f192 f216 f240 f264 f288 f312 f336 f360 f384'
  params='c00 mean prob spread'
  for fcst in $fcsts; do
    for param in $params; do
	  pattern="${GEFS}/*/wave/gridded/gefs.wave.t*z.${param}.global.0p25.${fcst}.grib2"
	  cp -p --no-clobber ${pattern} . &
	done
  done
  wait

  # sync up with push to Hera archives
  rsync -av --timeout=300 $TRANSFER/incoming/ ${RUSER}:$REMOTE_IN
  
  # clean up transfer archives
  rm -rf $ARCDIR
  rm -rf $GEFS_ARC

fi

echo "relay $STAGE completed at `date`"

exit
