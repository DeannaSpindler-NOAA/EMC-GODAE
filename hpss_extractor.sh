#!/bin/bash
#
# crush nc-3 files into nc4 classic file
#

echo "Starting: `date`"

module load gnu/9.2.0 netcdf/4.7.2 hpss

SRCDIR=/scratch2/NCEPDEV/ocean/Deanna.Spindler/save

theDate=$1
yy=`date --date=$theDate "+%Y"`
yymm=`date --date=$theDate "+%Y%m"`

WORKDIR=/scratch2/NCEPDEV/stmp1/Deanna.Spindler
ncdir=$WORKDIR/hpss/$theDate
ncout=$WORKDIR/cdo/$theDate
ARCHDIR=$NEWHOME/Global/archive/${theDate}

#hpss="/NCEPPROD/1year/hpssprod/runhistory/rh${yy}/${yymm}/${theDate}/com_rtofs_prod_rtofs.${theDate}.nc.tar"
hpss="/NCEPPROD/1year/hpssprod/runhistory/rh${yy}/${yymm}/${theDate}"

# track down the file for that day
hpssfile=$(hpsstar dir $hpss | grep rtofs |grep -v idx | grep nc | awk '{print $9}')

hpsstar=$SRCDIR/VPPPG/Global_RTOFS/EMC_ocean-prod-gen/dataproc/scripts/hpsstar

fcsts='n048 n024 f024 f048 f072 f096 f120 f144 f168 f192'
params='3ztio 3zsio 3zuio 3zvio diag prog ice'

#make the scrub directory

mkdir -p $ncdir
mkdir -p $ncout

# begin hpss extraction and crush the netcdf files

cd $ncdir
for fcst in $fcsts; do
  for param in $params; do
    for file in $( $hpsstar inx $hpss/$hpssfile | grep $param | grep $fcst ); do
	  fname=$(basename $file)
	  if [[ ! -e $ARCHDIR/$fname ]]; then
	    $hpsstar getnostage $hpss/$hpssfile ./$fname
		fname2=${fname/1hrly/daily}
		fname2=${fname2/3hrly/daily}
		mv $fname $fname2
		nccopy -7 -d 4 $fname2 $ncout/$fname2 &
	  fi
	done
  done  
done

wait

rm -rf $ncdir &

echo "Finished: `date`"
exit
