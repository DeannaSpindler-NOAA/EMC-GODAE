#!/bin/ksh -l
#-------------------------------------------------------
# GODAE IV/TT Dataset Generation Workflow
# Version 1.0
# Todd Spindler
# 22 Oct 2020
#-------------------------------------------------------

# set up module environment
module use /scratch2/NCEPDEV/ocean/Deanna.Spindler/save/modulefiles
module purge
module load intel/19.0.5.281 impi/2022.1.2
module load anaconda-xesmf/1.0.0
#module list

if [[ -n $1 ]]; then
  THE_DATE=$1
else
#  THE_DATE=`date --date='-6 days' +%Y%m%d`
  THE_DATE=`date --date='-7 days' +%Y%m%d`
fi

STOP_DATE=${2:-$THE_DATE}

echo "Starting GODAE for $THE_DATE to $STOP_DATE"

TASK_QUEUE='batch'
WALL='0:30:00'
#PROJ='marine-cpu'
PROJ='ovp'
LOGPATH=/scratch2/NCEPDEV/stmp1/Deanna.Spindler/logs/godae
PROFILEPATH=/scratch2/NCEPDEV/stmp1/Deanna.Spindler
JOB="godae"

mkdir -p $LOGPATH
#rm -f $LOGPATH/*.log

# clean out the weights from any previous run before we begin
rm -f $PROFILEPATH/godae/*.nc

#cp -r $HOME/.ipython/profile_xesmf $PROFILEPATH/profile_default
#export IPYTHONDIR=${PROFILEPATH}

SRCDIR='/scratch2/NCEPDEV/ocean/Deanna.Spindler/save/VPPPG/Global_RTOFS/EMC_ocean-verification/godae'

NEWHOME='/scratch2/NCEPDEV/ocean/Deanna.Spindler/save'

while (( $THE_DATE <= $STOP_DATE )); do
  # get the data
  /scratch2/NCEPDEV/ocean/Deanna.Spindler/save/bin/get_godae.sh $THE_DATE
  echo "Submitting job for $THE_DATE"
  job1=$(sbatch --parsable -J ${JOB}_1_${THE_DATE} -o $LOGPATH/${JOB}_1_${THE_DATE}.log -q $TASK_QUEUE --account=$PROJ --time $WALL --ntasks=4 --nodes=1 --wrap "python $SRCDIR/ush/godae_rtofsv2.py $THE_DATE profile")
  job2=$(sbatch --parsable -J ${JOB}_2_${THE_DATE} -o $LOGPATH/${JOB}_2_${THE_DATE}.log -q $TASK_QUEUE --account=$PROJ --time $WALL --ntasks=4 --nodes=1 --wrap "python $SRCDIR/ush/godae_rtofsv2.py $THE_DATE SLA")
  job3=$(sbatch --parsable -J ${JOB}_3_${THE_DATE} -o $LOGPATH/${JOB}_3_${THE_DATE}.log -q $TASK_QUEUE --account=$PROJ --time $WALL --ntasks=4 --nodes=1 --wrap "python $SRCDIR/ush/godae_rtofsv2.py $THE_DATE SST")
  job4=$(sbatch --parsable -J ${JOB}_4_${THE_DATE} -o $LOGPATH/${JOB}_4_${THE_DATE}.log -q $TASK_QUEUE --account=$PROJ --time $WALL --ntasks=4 --nodes=1 --wrap "python $SRCDIR/ush/godae_rtofsv2.py $THE_DATE aice")
  # this last one uploads to GODAE, needs to run after testing jobs 1-4 work.
  job5=$(sbatch --parsable --dependency=afterok:${job1}:${job2}:${job3}:${job4} --partition=service -J ${JOB}_transfer_${THE_DATE} -q $TASK_QUEUE --account=$PROJ --time $WALL --ntasks 1 -o $LOGPATH/transfer_${THE_DATE}.log --wrap "$SRCDIR/scripts/upload_godae.sh $THE_DATE")
  THE_DATE=$(date --date="${THE_DATE} +1day" +%Y%m%d)  
done

