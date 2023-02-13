#!/bin/ksh -l
#
# download and extract GODAE data using https
#

module use /scratch2/NCEPDEV/ocean/Deanna.Spindler/save/modulefiles
module load anaconda-work/1.0.0

function godae {
  YEAR=$(date --date=${GODAE_DATE} +%Y)  
  DATA_DIR='/scratch2/NCEPDEV/ocean/Deanna.Spindler/noscrub/GODAE/incoming'  
  mkdir -p $DATA_DIR
  cd $DATA_DIR
  # get everything for that date
  wget -q https://usgodae.org/pub/outgoing/GODAE_class4/${YEAR}/${GODAE_DATE}/class4_${GODAE_DATE}_FOAM_orca025_14.1_SLA.nc.gz.enc
  wget -q https://usgodae.org/pub/outgoing/GODAE_class4/${YEAR}/${GODAE_DATE}/class4_${GODAE_DATE}_FOAM_orca025_14.1_SST.nc.gz.enc
  wget -q https://usgodae.org/pub/outgoing/GODAE_class4/${YEAR}/${GODAE_DATE}/class4_${GODAE_DATE}_FOAM_orca025_14.1_profile.nc.gz.enc
  wget -q https://usgodae.org/pub/outgoing/GODAE_class4/${YEAR}/${GODAE_DATE}/class4_${GODAE_DATE}_GIOPS_CONCEPTS_3.3_aice.nc.gz.enc
  # convert everything to nc
  for file in *${GODAE_DATE}*.enc; do
    FNAME=$(basename $file .gz.enc) 
	# 20230208: all GODAE files from 20221227 onwards changed from digest='' to digest='-md sha256'
	# Mercator uses a newer openssl digest hash
	#if [[ $FNAME =~ 'PSY4' ]]; then
	  digest='-md sha256'
	#else
	#  digest=''
	#fi
    /usr/bin/openssl enc -d -aes-256-cbc $digest -pass pass:ire15aus6 -in ${FNAME}.gz.enc | zcat > ${FNAME}
    nccopy -7 -d 4 ${FNAME} ${FNAME}4
    mv ${FNAME}4 ${FNAME}
    rm ${FNAME}.gz.enc
  done  
}

# main routine

START_DATE=${1:-$(date --date='yesterday' +%Y%m%d)}
STOP_DATE=${2:-$START_DATE} 

while (( $START_DATE <= $STOP_DATE )); do  
  echo "processing $START_DATE"
  WANT_DATE=$(date --date=${START_DATE} +%Y-%m-%d)
  if [[ -n $1 ]]; then
    GODAE_DATE=${START_DATE}
  else
    GODAE_DATE=$(date --date="${START_DATE} -6days" +%Y%m%d)  
  fi
  godae  
  START_DATE=$(date --date="${START_DATE} +1day" +%Y%m%d)
done

