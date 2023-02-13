#!/bin/env python
"""
GODAE OceanPredict IV/TT Dataset Processing 
version: 1.0
 author: Todd Spindler
   date: 27 Oct 2020
   
Notes
    Four types:  profile, SST, SLA, aice (ice)
    Processing sequence:
        Load appropriate GODAE dataset
        Load 8 days of Global RTOFS data (open_mfdataset)
        	Note that profiles are 3D, SLA, SST, and ice are 2D
        Interpolate MDT from HYCOM 30-year average for SLA calculation
        Interpolate RTOFS to GODAE locations (2D bilinear,linear in Z)
            Interpolate lat/lon locations for each data set
            Interpolate depths for profiles
        Interpolate best estimate (nowcast at valid date)
        Interpolate persistence (nowcast at forecast runtime)
        Copy GODAE nc file and open for modification (actually create new one from xarray)
        Load new data into nc file
        Compress and encrypt nc file
        
        In separate V&V job:
        Plot profiles, SST, SLA, and ice concentration values
        Compute statistics and save to dbase
        Plot accumulated statistics
        
    Working Definitions:
        MDT -- Mean SSH from HYCOM 30-year avg
        Persistence -- nowcast from run date for forecast        
        Best Estimate -- nowcast from valid date

Versions
	1.0 -- first production release.  Added additional file not found condition handlers.
    1.0.1 -- removed combine_attrs from xr.merge due to newer version of xarray, Aug 2021.

"""
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import xarray as xr
import xesmf as xe
import numpy as np
import scipy.interpolate as sip
from datetime import datetime, timedelta
import subprocess
import os, sys
#import ipdb

baseDir='/scratch2/NCEPDEV/ocean/Deanna.Spindler/noscrub'
fixDir=''
tempDir='/scratch2/NCEPDEV/stmp1/Deanna.Spindler'
#imageDir=f'{tempDir}/images/class-4/godae'
climoDir=f'{baseDir}/Global/climo/HYCOM'
godaeDir=f'{baseDir}/GODAE'
modelDir=f'{baseDir}/Global/archive'

#----------------------------------------------------------------
def godae_fix(data,param):
    """
    Fix the bad GODAE data sets
    """
    def nan2nat(ref,day):
        #convert nan to NaT
        td = pd.NaT
        if not np.isnan(day):
            td = pd.Timedelta(days=day)
        return pd.Timestamp(ref) + td
        
    jd = [nan2nat(data[param].units.split()[2],day) for day in data[param].values.tolist()]
    ds = xr.Dataset({param: (data[param].dims, jd, {'units': 'ns'})}) # Update the units attribute appropriately
    ds = xr.decode_cf(ds)
    ds[param].attrs={}
    data[param]=ds[param]
    return data

#----------------------------------------------------------------
def get_godae(theDate,parameter):
    """
    Decrypt the ncfile and read into memory
    parameter='profile','SLA','SST','aice'
    """
    if parameter=='aice':        
        filename=f'class4_{theDate:%Y%m%d}_GIOPS_CONCEPTS_3.3_{parameter}.nc'
    else:
        filename=f'class4_{theDate:%Y%m%d}_FOAM_orca025_14.1_{parameter}.nc'
        
    fn=f'{godaeDir}/incoming/{filename}'
    if os.path.exists(fn):
        try:
            data=xr.open_dataset(fn,decode_times=True)        
            return data
        except:
            try:
                data=xr.open_dataset(fn,decode_times=False)
                data=godae_fix(data,'juld')
                data=godae_fix(data,'modeljuld')
                return data
            except:
                print('Problem opening file:',filename)
                return None 
    else:
        print('File Not Found:',filename)
        return None
#----------------------------------------------------------------
def get_rtofs(vDate,obs,wantPersist=False):
    """
    Load nowcast/fcst from local archive.  wantPersist gets nowcasts aligned with 
    valid date.

    The FOAM (UKMET) GODAE data set has daily mean fields centered at
	12Z.  RTOFS doesn't produce daily means, but we can estimate it by
	averaging between two model days to get the 00Z mean field.

    RTOFS v2 now reports valid time in MT, but no forecast hour.  File open needs to be multi-step,
    with each forecast separately loaded, and each forecast appended or concatenated to form the full
    dask array.
    
    """
    template='{}/{}/rtofs_glo_{}_{}{:03n}_{}.nc'
    fnames=[]
    data=xr.Dataset()
    # nowcasts and forecasts out to 168 ... only need out to 144
    all_fcsts=np.arange(0,193,24)
    #all_fcsts=np.arange(0,145,24)
    all_fnames={}
    for fcst in all_fcsts:
        fnames=[]
        runDate=vDate-timedelta(fcst/24.)
        ftype='f'
        fcst_hrs=fcst
        # nowcast names are different.  This changes for rtofsv2
        if fcst == 0:
            ftype='n'
            fcst_hrs=24
        
        if obs.obs_type=='profile':
            if wantPersist:
                fnames.extend([template.format(modelDir,runDate.strftime('%Y%m%d'),'3dz','n',24,i) for i in ['daily_3ztio','daily_3zsio']])
            else:
                fnames.extend([template.format(modelDir,runDate.strftime('%Y%m%d'),'3dz',ftype,fcst_hrs,i) for i in ['daily_3ztio','daily_3zsio']])
            all_fnames[fcst]=fnames[-2:]
        else:
            if obs.obs_type=='SST':
                if wantPersist:
                    fnames.append(template.format(modelDir,runDate.strftime('%Y%m%d'),'2ds','n',24,'prog'))
                else:
                    fnames.append(template.format(modelDir,runDate.strftime('%Y%m%d'),'2ds',ftype,fcst_hrs,'prog'))
            elif obs.obs_type=='SLA':
                if wantPersist:
                    fnames.append(template.format(modelDir,runDate.strftime('%Y%m%d'),'2ds','n',24,'diag'))
                else:
                    fnames.append(template.format(modelDir,runDate.strftime('%Y%m%d'),'2ds',ftype,fcst_hrs,'diag'))
            elif obs.obs_type=='AMSR2 brightness temperature': # this is ice
                if wantPersist:
                    fnames.append(template.format(modelDir,runDate.strftime('%Y%m%d'),'2ds','n',24,'ice'))
                else:
                    fnames.append(template.format(modelDir,runDate.strftime('%Y%m%d'),'2ds',ftype,fcst_hrs,'ice'))
            all_fnames[fcst]=fnames[-1]
        ds=xr.open_mfdataset(fnames,decode_times=True)
        
        # set MT from valid time to run time, as it used to be
        ds=ds.squeeze()
        ds.coords['MT']=runDate
        if data.nbytes==0:
            data=ds.copy()
        else:
            # removed combine_attrs since newer version of xarray, Aug 2021
            #data=xr.concat([data,ds],'MT',combine_attrs='no_conflicts')
            data=xr.concat([data,ds],'MT')
    
    data=data.rename({'Longitude':'lon','Latitude':'lat'})
    data=data.squeeze()
    
    if obs.obs_type=='SST':
        data=data['sst'].to_dataset()
    elif obs.obs_type=='SLA':
        data=data['ssh'].to_dataset()
    elif obs.obs_type=='AMSR2 brightness temperature':
        data=data['ice_coverage'].to_dataset()
        data=data.rename({'ice_coverage':'ice'})  # match param to varname
                        
    data.coords['forecast']=(('nfcst',),all_fcsts)
    data.attrs['input_files']=all_fnames
    data['lon'][-1,]=data.lon[-2,]  # the old lon fix

    return data
#----------------------------------------------------------------
def get_hycom_climo(vDate,rtofs):
    """
    get MDT from 30-year HYCOM dataset and add it as mdt_reference
    
    Read HYCOM 1995-2015 mean data
    This is used for the mssh values
    
    HYCOM monthly data is centered on mid-month, scale theDate against
	the date range to create a weighted average of the straddling
	month fields.    
    """

    if vDate.day==15:  # even for Feb, just because
        climofile="hycom_GLBv0.08_53X_archMN.1994_{0:02n}_2015_{0:02n}_ssh.nc".format(vDate.month)
        data=xr.open_dataset(climoDir+'/'+climofile,decode_times=False)
        data=data['surf_el'].copy().squeeze()
    else:  # need to scale things
        if vDate.day < 15:
            start=pd.Timestamp(vDate.year,vDate.month,15)+pd.tseries.offsets.DateOffset(months=-1)
            stop=pd.Timestamp(vDate.year,vDate.month,15)
        else:
            start=pd.Timestamp(vDate.year,vDate.month,15)
            stop=pd.Timestamp(vDate.year,vDate.month,15)+pd.tseries.offsets.DateOffset(months=1)
        left=(vDate-start)/(stop-start)
        #right=(stop-vDate)/(stop-start)
        climofile1="hycom_GLBv0.08_53X_archMN.1994_{0:02n}_2015_{0:02n}_ssh.nc".format(start.month)
        climofile2="hycom_GLBv0.08_53X_archMN.1994_{0:02n}_2015_{0:02n}_ssh.nc".format(stop.month)
        data1=xr.open_dataset(climoDir+'/'+climofile1,decode_times=False)
        data2=xr.open_dataset(climoDir+'/'+climofile2,decode_times=False)
        data1=data1['surf_el'].copy().squeeze()
        data2=data2['surf_el'].copy().squeeze()                    
        #data=data1*left+data2*right
        data=data1+((data2-data1)*left)
        climofile='weighted average of '+climofile1+' and '+climofile2
    return data
#-------------------------------------------------
def get_regridder(model,obs,vDate,param=None):
    print('initializing xESMF regridder')
    weightfile=f'{tempDir}/godae/godae_{obs.obs_type.replace(" ","_")}_{param}_{vDate:%Y%m%d}_weights.nc'
    xobs=xr.Dataset()
    xobs.coords['lon']=obs.longitude
    xobs.coords['lat']=obs.latitude
    if obs.obs_type != 'AMSR2 brightness temperature':
        xobs.coords['depth']=obs.depth
    #xobs['observation']=obs.observation[:,0,:]
    # rename model coords as needed
    regridder=xe.Regridder(model,xobs,'bilinear',
                           filename=weightfile,
                           ignore_degenerate=True,
                           locstream_out=True)
    return regridder
#-------------------------------------------------
def depth_interp(model,obs):
    """
    this routine expects a DataArray
    """
    
    tall=np.array([])
    for nob in obs.numobs:        
        tnew=np.array([])
        snew=np.array([])
        for nt in range(model.MT.size):
            model2=model.isel(MT=nt).copy()
            obs_depth=obs.depth[nob].values
            ft=sip.interp1d(model2.Depth.values,model2[:,nob].values,bounds_error=False)
            if tnew.size==0: 
                tnew=ft(obs.depth[nob].values)
            else:
                tnew=np.vstack((tnew,ft(obs.depth[nob].values)))
                
        if tall.size==0:
            tall=tnew
        else:
            tall=np.dstack((tall,tnew))

    tall=tall.swapaxes(2,1)
    return tall
#----------------------------------------------------------------
def create_profile_dataset(model,persist,best_estimate,obs):
    """
    Important to remember
    model and persist times are 24hr fcst onward, or [1:7] in forecast array.  
    Skip the 0 hour fcst, even for persist.  0Z persist is best_estimate.
    """
    
    # begin with the depth interpolation
    fcst_t=depth_interp(model.temperature,obs)
    fcst_s=depth_interp(model.salinity,obs)
    
    persist_t=depth_interp(persist.temperature,obs)
    persist_s=depth_interp(persist.salinity,obs)
    
    best_estimate_t=depth_interp(best_estimate.temperature,obs)
    best_estimate_s=depth_interp(best_estimate.salinity,obs)

    # copy the obs dataset and load it with model values
    # this assumes that forecast is present, but makes no
    # assumptions about persistence or best_estimate, which
    # is sometimes missing from obs dataset
    obs2=obs.copy(deep=True)    
        
    # null out the fields first
    obs2['forecast']=obs2['forecast']*np.nan
    if 'persistence' not in obs2:
        obs2['persistence']=xr.ones_like(obs2.forecast)*np.nan
    else:
        obs2['persistence']=obs2['persistence']*np.nan
    if 'best_estimate' not in obs2:
        obs2['best_estimate']=xr.ones_like(obs.climatology)*np.nan
    else:
        obs2['best_estimate']=obs2['best_estimate']*np.nan

    obs2['forecast'][:,0,]=fcst_t[1:7,].swapaxes(0,1)
    obs2['forecast'][:,1,]=fcst_s[1:7,].swapaxes(0,1)    
    obs2['persistence'][:,0,]=persist_t[1:7,].swapaxes(0,1)
    obs2['persistence'][:,1,]=persist_s[1:7,].swapaxes(0,1)
    obs2['best_estimate'][:,0,]=best_estimate_t.squeeze()
    obs2['best_estimate'][:,1,]=best_estimate_s.squeeze()

    # add comments to the metadata
    obs2['forecast'].attrs['comment']='12Z time average of 00Z model forecast and forecast+24h daily instantaneous fields'
    obs2['persistence'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'
    obs2['best_estimate'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'

    # add times
    obs2['modeljuld']=obs.modeljuld.to_series()
    obs2['leadtime'][:]=model.forecast[1:7]
    
    return obs2
#----------------------------------------------------------------
def create_SST_dataset(model,persist,best_estimate,obs):

    obs2=obs.copy(deep=True)
    
    # null out the fields first
    obs2['forecast']=obs2['forecast']*np.nan
    if 'persistence' not in obs2:
        obs2['persistence']=xr.ones_like(obs2.forecast)*np.nan
    else:
        obs2['persistence']=obs2['persistence']*np.nan
    if 'best_estimate' not in obs2:
        obs2['best_estimate']=xr.ones_like(obs.climatology)*np.nan
    else:
        obs2['best_estimate']=obs2['best_estimate']*np.nan
    
    obs2['forecast'][:]=model.sst[1:7,].values.swapaxes(0,1)[:,np.newaxis,:,np.newaxis]
    obs2['persistence'][:]=persist.sst[1:7,].values.swapaxes(0,1)[:,np.newaxis,:,np.newaxis]
    obs2['best_estimate'][:]=best_estimate.sst.values.swapaxes(0,1)[:,:,np.newaxis]

    # add times
    obs2['modeljuld']=obs.modeljuld.to_series()
    obs2['leadtime'][:]=model.forecast[1:7]
    
    # add comments to the metadata
    obs2['forecast'].attrs['comment']='12Z time average of 00Z model forecast and forecast+24h daily instantaneous fields'
    obs2['persistence'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'
    obs2['best_estimate'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'
    
    return obs2
#----------------------------------------------------------------
def create_SLA_dataset(model,persist,best_estimate,climo,obs):
    
    obs2=obs.copy(deep=True)
        
    # null out the fields first
    obs2['forecast']=obs2['forecast']*np.nan
    if 'persistence' not in obs2:
        obs2['persistence']=xr.ones_like(obs2.forecast)*np.nan
    else:
        obs2['persistence']=obs2['persistence']*np.nan
    if 'best_estimate' not in obs2:
        obs2['best_estimate']=xr.ones_like(obs.climatology)*np.nan
    else:
        obs2['best_estimate']=obs2['best_estimate']*np.nan
        
    obs2['forecast'][:]=model.sla[1:7,].values.swapaxes(0,1)[:,np.newaxis,:,np.newaxis]
    obs2['persistence'][:]=persist.sla[1:7,].values.swapaxes(0,1)[:,np.newaxis,:,np.newaxis]
    obs2['best_estimate'][:]=best_estimate.sla.values.swapaxes(0,1)[:,:,np.newaxis]
    obs2['mdt_reference'][:]=climo.values[:,np.newaxis,np.newaxis]

    # add times
    obs2['modeljuld']=obs.modeljuld.to_series()
    obs2['leadtime'][:]=model.forecast[1:7]

    # add comments to metadata
    obs2['forecast'].attrs['comment']='12Z time average of 00Z model forecast and forecast+24h daily instantaneous fields'
    obs2['persistence'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'
    obs2['best_estimate'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'
    obs2['mdt_reference'].attrs['comment']='HYCOM 30-yr monthly mean SSH interpolated to the correct day'

    return obs2
#----------------------------------------------------------------
def create_ice_dataset(model,persist,best_estimate,obs):
    
    obs2=obs.copy(deep=True)
        
    # null out the fields first
    obs2['forecast']=obs2['forecast']*np.nan
    if 'persistence' not in obs2:
        obs2['persistence']=xr.ones_like(obs2.forecast)*np.nan
    else:
        obs2['persistence']=obs2['persistence']*np.nan
    if 'best_estimate' not in obs2:
        obs2['best_estimate']=xr.ones_like(obs.climatology)*np.nan
    else:
        obs2['best_estimate']=obs2['best_estimate']*np.nan
        
    obs2['forecast'][:,:,0:9,]=model.ice.values.swapaxes(0,1)[:,np.newaxis,:,np.newaxis]
    obs2['persistence'][:,:,0:9,]=persist.ice.values.swapaxes(0,1)[:,np.newaxis,:,np.newaxis]
    obs2['best_estimate'][:]=best_estimate.ice.values.swapaxes(0,1)[:,:,np.newaxis]
    
    # add times
    #obs2['modeljuld']=obs.modeljuld.to_series()
    #obs2['leadtime'][:]=model.forecast[1:7]
    
    # add comments to metadata
    obs2['forecast'].attrs['comment']='12Z time average of 00Z model forecast and forecast+24h daily instantaneous fields'
    if 'persistence' not in obs2:
        obs2['persistence']=xr.ones_like(obs2.forecast)*np.nan    
    obs2['persistence'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'
    if 'best_estimate' not in obs2:
        obs2['best_estimate']=xr.ones_like(obs.climatology)*np.nan
    obs2['best_estimate'].attrs['comment']='12Z time average of 00Z model nowcast and nowcast+24h daily instantaneous fields'
    
    return obs2
# main routine starts here                                       
if __name__ == '__main__':
    
    theDate=pd.Timestamp(sys.argv[1])
    param=sys.argv[2]  # profile, SST, SLA, aice
    
    obs=get_godae(theDate,param)
    
    # if file not found, exit immediately
    if obs is None:
        print('Exiting now')
        sys.exit(0)
        
    data_keys={'profile':['temperature','salinity'],
               'aice':['ice'],
               'SLA':['ssh'],
               'SST':['sst']}

    # get rtofs forecast data from today and tomorrow (for 12Z mean)
    model1=get_rtofs(theDate,obs,wantPersist=False)
    model2=get_rtofs(theDate+timedelta(1),obs,wantPersist=False)
    
    model1=model1.reset_index('MT')
    model2=model2.reset_index('MT')    
    model=xr.full_like(model1,fill_value=np.nan)
    for key in data_keys[param]:
        model[key]=(model1[key]+model2[key])/2.
    model['MT_']=model.MT_.to_pandas()+pd.Timedelta('0.5d')        
    model=model.set_index({'MT':'MT_'})
    del model1, model2    
        
    # regrid model to GODAE
    model.load()
    model_regridder=get_regridder(model,obs,theDate,'model')
    model=model_regridder(model,keep_attrs=True)
    
    if param=='SLA':
        climo=get_hycom_climo(theDate,model)    
        climo.load()
        climo_regridder=get_regridder(climo,obs,theDate,'climo')
        climo=climo_regridder(climo,keep_attrs=True)  # need climo regridded for SLA calc
        model['sla']=model.ssh-climo
        del model['ssh']
            
    # get rtofs nowcast data from today and tomorrow (for 12Z mean)
    persist1=get_rtofs(theDate,obs,wantPersist=True)
    persist2=get_rtofs(theDate+timedelta(1),obs,wantPersist=True)
    
    persist1=persist1.reset_index('MT')
    persist2=persist2.reset_index('MT')    
    persist=xr.full_like(persist1,fill_value=np.nan)
    for key in data_keys[param]:
        persist[key]=(persist1[key]+persist2[key])/2.
    persist['MT_']=persist.MT_.to_pandas()+pd.Timedelta('0.5d')
    persist=persist.set_index({'MT':'MT_'})
    del persist1, persist2

    # best estimate is avg of today and tomorrow's nowcast
    best_estimate=persist.isel({'MT':[0]}) 
    
    # regrid model persistence to GODAE
    persist.load()    
    persist=model_regridder(persist,keep_attrs=True)

    # regrid model best_estimate to GODAE
    best_estimate.load()
    best_estimate=model_regridder(best_estimate,keep_attrs=True)
    
    if param=='SLA':
        persist['sla']=persist.ssh-climo
        best_estimate['sla']=best_estimate.ssh-climo
        del persist['ssh'], best_estimate['ssh']
                             
    # substitute model data into obs dataset 
    if param=='profile':
        obs2=create_profile_dataset(model,persist,best_estimate,obs)
    elif param=='SST':
        obs2=create_SST_dataset(model,persist,best_estimate,obs)
    elif param=='SLA':
       obs2=create_SLA_dataset(model,persist,best_estimate,climo,obs)
    elif param=='aice':
       obs2=create_ice_dataset(model,persist,best_estimate,obs)
    else:
        print('Unrecognized parameter.  Exiting')

    # cleanup - not available with xesmf 0.6.0
    #model_regridder.clean_weight_file()
    #if param=='SLA':
    #    climo_regridder.clean_weight_file()

    version=2.0
        
    # write out in netcdf-3 (classic) format
    obs2.attrs['creation_date']=f'{datetime.now()} UTC'
    obs2.attrs['contact']='Deanna.Spindler@noaa.gov'
    obs2.attrs['system']='RTOFS Global Ocean Model v2.0'
    obs2.attrs['configuration']='HYCOM 1/12 deg tripolar with data assimilation'
    obs2.attrs['institution']='NOAA/NWS/NCEP/Environmental Modeling Center'
    obs2.attrs['validity_time']=f'{theDate} UTC'
    obs2.attrs['best_estimate_description']='12Z Time average of 00Z nowcast and nowcast+24h daily instantaneous fields'
    obs2.attrs['time_interp']='12Z time average of 00Z at valid time and valid time + 24h daily instantaneous fields'
    obs2.attrs['version']=version
    if param=='aice':
        obs2.attrs['Ice_concentration_generation_method']='Sea Ice Area Fraction from CICE-4 model coupled to HYCOM'
    
    # delete unnecessary attributes
    if 'suite' in obs2.attrs:
        del obs2.attrs['suite'], obs2.attrs['suite_number']

    ncfile=f'{godaeDir}/outgoing/class4_{theDate:%Y%m%d}_HYCOM_RTOFS_{version}_{param}.nc'
        
    encoding={key:{'zlib':True} for key in obs2.keys()}
    # juld and modeljuld need new units
    if param != 'aice':
        encoding['juld']['units']='Days since 1950-01-01 00:00:00 UTC'
        encoding['modeljuld']['units']='Days since 1950-01-01 00:00:00 UTC'
    else:
        encoding['obs_time']['units']='Days since 1950-01-01 00:00:00 UTC'
    
    obs2.to_netcdf(ncfile,format='NETCDF3_CLASSIC',encoding=encoding)
    
    del obs, obs2, model, model_regridder
    
    # compress and encrypt the file in preparation for upload
    subprocess.call(f'/usr/bin/gzip -c {ncfile} | /usr/bin/openssl enc -e -aes-256-cbc -salt -pass pass:ire15aus6 -out {ncfile}.gz.enc',shell=True)
    
    #openssl enc -aes-256-cbc -salt -pass pass:<passwd> -in file.txt -out file.enc
