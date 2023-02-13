#!/bin/ksh
#
# Transfer GODAE data sets to USGODAE from front end node hfe01
#

NEWHOME='/scratch2/NCEPDEV/ocean/Deanna.Spindler'
GODAEHOME='/scratch2/NCEPDEV/ocean/Deanna.Spindler/noscrub/GODAE/outgoing'
VERSION='2.0'

theDate=${1:-$(date --date='-6 days' +%Y%m%d)}
stopDate=${2:-$theDate}  

echo "uploading from $theDate to $stopDate"

while (( $theDate <= $stopDate )); do
  for filetype in profile aice SST SLA; do
    if [[ -a $GODAEHOME/class4_${theDate}_HYCOM_RTOFS_${VERSION}_${filetype}.nc.gz.enc ]]; then
      echo "Beginning upload of $filetype at `date`"
	  if [[ $HOSTNAME == 'hfe01' ]]; then
        /usr/bin/curl -T $GODAEHOME/class4_${theDate}_HYCOM_RTOFS_${VERSION}_${filetype}.nc.gz.enc ftp://ftp.usgodae.org/pub/incoming/class4/
	  else
	    ssh hfe01 /usr/bin/curl -T $GODAEHOME/class4_${theDate}_HYCOM_RTOFS_${VERSION}_${filetype}.nc.gz.enc ftp://ftp.usgodae.org/pub/incoming/class4/
		#curl -v -T $POLARHOME/class4_${theDate}_HYCOM_RTOFS_${VERSION}_${filetype}.nc.gz.enc ftp://usgodae.org/pub/incoming/class4/
	  fi
      echo "Finished at `date`"
    fi
  done
  theDate=$(date --date="$theDate +1day" "+%Y%m%d")
done
  
exit              
