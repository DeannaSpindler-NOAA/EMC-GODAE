# EMC-GODAE
Produces the Global RTOFS files for the GODAE ocean task group.

Start: godae.sh

The process is run 7 days behind, because it requires 4 files to be 
available from https://usgodae.org/pub/outgoing/GODAE_class4/2023:

class4_YYYYMMDD_FOAM_orca025_14.1_SLA.nc.gz.enc

class4_YYYYMMDD_FOAM_orca025_14.1_SST.nc.gz.enc 

class4_YYYYMMDD_FOAM_orca025_14.1_profile.nc.gz.enc

class4_YYYYMMDD_GIOPS_CONCEPTS_3.3_aice.nc.gz.enc 

Procedures expects 2 months of Global RTOFS data in the archive
and the climatology files {baseDir}/Global/climo/HYCOM
