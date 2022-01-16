#%%
import sys
from lxml import etree
from pandas import DataFrame, Series, to_datetime
import datetime as dt
import matplotlib.pyplot as plt
from pathlib import Path
from numpy import nan


def get_activities_list(folder_name):
    '''
    Get a list of garmin .tcx files from specified folder in current dir

    Args: folder(str)
    Returns: list(Path)
    '''
    dir = Path.cwd() / folder_name
    files = list(dir.glob('*.tcx'))
    activities_list = sorted(files, key=lambda path: int(path.stem.rsplit('_', 1)[1]))
    
    return activities_list

def parse_garmin_tcx(filename):
    """ Parses tcx activity file from Garmin Connect to Pandas DataFrame object
    Args: filename (str) - tcx file

    Returns: a tuple of id(str) and data(DataFrame)
    DF columns=['time'(datetime.time), 'distance, m'(float), 'HR'(int), 
    'cadence'(int), 'speed, m/s'(int)]
    """
    tree = etree.parse(str(filename))

    # set namespaces for garmin tcx file
    ns = {'ns0': '{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}',
    'ns3': '{http://www.garmin.com/xmlschemas/ActivityExtension/v2}'}

    id = to_datetime(tree.find('.//' + ns['ns0'] + 'Id').text).date()

    trackpoints = tree.findall('.//' + ns['ns0'] + 'Trackpoint')

    data = DataFrame(columns='time,distance,HR,speed,cadence,latitude,longitude,altitude'.split(','))    

    for n, trackpoint in enumerate(trackpoints):
        data.loc[n, 'time'] = trackpoint.find('.//' + ns['ns0'] + 'Time').text
        data.loc[n, 'distance'] = float(trackpoint.find('.//' + ns['ns0'] + 'DistanceMeters').text)
        data.loc[n, 'altitude'] = float(trackpoint.find('.//' + ns['ns0'] + 'AltitudeMeters').text)
        data.loc[n, 'HR'] = int(trackpoint.find('.//' + ns['ns0'] + 'HeartRateBpm/').text)
        try:
            data.loc[n, 'latitude'] = float(trackpoint.find('.//' + ns['ns0'] + 'LatitudeDegrees').text)
        except:
            data.loc[n, 'latitude'] = nan
        try:
            data.loc[n, 'longitude'] = float(trackpoint.find('.//' + ns['ns0'] + 'LongitudeDegrees').text)
        except:
            data.loc[n, 'longitude'] = nan
        try:
            data.loc[n, 'speed'] = float(trackpoint.find('.//' + ns['ns3'] + 'Speed').text)
        except:
            data.loc[n, 'speed'] = nan
        try:
            data.loc[n, 'cadence'] = int(trackpoint.find('.//' + ns['ns3'] + 'RunCadence').text) * 2
        except:
            data.loc[n, 'cadence'] = nan

    data.loc[:,'time'] = to_datetime(data['time'])

    return (id, data)

def clean_garmin_tcx_data(data):
    '''
    Replace zero cadence and speed value, calculate pace,
    transform astronomic time to time from the beginning of activity 

    Args: data(pd.DataFrame) - data from parse_garmin_tcx() function
    Returns: data(pd.DataFrame)
    '''
    #fill not recorded or missing data (previous not null value)
    data[['speed', 'cadence', 'latitude', 'longitude', 'altitude']] = data[['speed', 'cadence', 'latitude', 'longitude', 'altitude']].replace(0, method='ffill')
    data.fillna(method='ffill', inplace=True)

    # calculate and format time scale
    data.loc[:, 'time'] = data.loc[:, 'time'] - data.loc[0, 'time']
    data.loc[:, 'time'] = data.loc[:, 'time'].apply(lambda x: x.total_seconds())

    return data

def extract_running_intervals(data):
    ''' 
    Extract running intervals from cleaned tcx data

    Args: data(pd.DataFrame) - data from clean_garmin_tcx_data() function
    Returns: running_intervals(pd.DataFrame) 
                columns=['start time', 'stop time', 'start dist', 'stop dist', 
                            'duration, s', 'distance, m']
    '''
    # separate running from walking intervals
    # calculate differences by data points
    intervals = data[['time', 'distance', 'HR', 'cadence', 'speed']].diff().dropna()
    intervals.columns = ['dtime', 'ddist', 'dHR', 'dcad', 'dspeed']
    intervals.drop(columns='dspeed', inplace=True)

    # find start/stop indices based on cadence increase/decrease level
    walkrun_cadence_threshold = 30
    max_walking_cadence = 125
    # TODO filter by 3 points - sum dcad will be greater than 40, then filter if was already running/walking
    # btw maybe filter is unnecessary because its unlikely that there'd be such increase in cadence on the run
    # pass intervals df to filter function, doing moving window filtering
    started_running = intervals[intervals['dcad'] > walkrun_cadence_threshold].index.values.tolist()
    stopped_running = intervals[intervals['dcad'] < -walkrun_cadence_threshold].index.values.tolist()

    # TODO: assert len(started) == len(stopped), intervals are consistent:
    # start time < stop time
    # when cadence falls from 200 to 160 (running), then to 120 program registers
    # 2 instances of stopping running -> modify condition
    for n, start in enumerate(started_running):
        # filter cadence increase while running
        if data.loc[start - 1, 'cadence'] > max_walking_cadence:
            started_running.pop(n)
        # filter cadence increase while walking too slow
        elif data.loc[start, 'cadence'] < max_walking_cadence:
            started_running.pop(n)
    for n, stop in enumerate(stopped_running):
        # filter cadence decrease while running
        if data.loc[stop, 'cadence'] > max_walking_cadence:
            stopped_running.pop(n)
        # filter cadence decrease while walking
        elif data.loc[stop - 1, 'cadence'] < max_walking_cadence:
            stopped_running.pop(n)
    print(f'{len(started_running)} start points\n{len(stopped_running)} stop points')
    
    # filter and save intervals data from dataset to separate dataframe
    running_intervals = DataFrame(columns=['start time', 'stop time', 'start dist', 
                                            'stop dist'])
    
    # TODO do only if len(start) = len(stop), else - clean 
    for start, stop in zip(started_running, stopped_running):
        start_time = data.loc[start, 'time']
        start_dist = data.loc[start, 'distance']
        stop_time = data.loc[stop, 'time']
        stop_dist = data.loc[stop, 'distance']
        
        running_intervals = running_intervals.append(Series({'start time': start_time, 
                                                    'stop time': stop_time,'start dist': start_dist, 
                                                    'stop dist': stop_dist}), ignore_index=True)
    
    # calculate intervals duration and distance covered
    running_intervals['duration'] = running_intervals['stop time'] - running_intervals['start time']
    running_intervals['distance'] = running_intervals['stop dist'] - running_intervals['start dist']
    # convert time from seconds to hh:mm:ss format
    running_intervals.loc[:, 'start time'] = running_intervals.loc[:, 'start time'].apply(
                                                lambda x: str(dt.timedelta(seconds=x)))
    running_intervals.loc[:, 'stop time'] = running_intervals.loc[:, 'stop time'].apply(
                                                lambda x: str(dt.timedelta(seconds=x)))

    return running_intervals

def plot_running_stats(data):
    '''
    Plot pace, cadence, hr vs time from garmin tcx dataframe 
    '''
    plt.close('all')
    plt.figure()
    data.plot(x='time', y='pace')
    plt.gca().invert_yaxis()
    plt.show()

    plt.figure()
    data.plot(x='time', y='cadence')
    plt.show()

    plt.figure()
    data.plot(x='time', y='HR')
    plt.show()
    return

#%%

# calculate hr variability (increase/decrease rate)
# hrv_rate = intervals['dHR'] / intervals['dtime']
# hr_time = data.iloc[:-1]['time'] + (intervals['dtime'].reset_index(drop=True) / 2)

#%%
def main(files): 
    # prepare canvas
    plt.close('all')
    fig, ax  = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))
    ax[0].title.set_text('Distance, m')
    ax[1].title.set_text('Duration, s')

    for file in files:
        try:
            #load data
            id, data = parse_garmin_tcx(file)
            data = clean_garmin_tcx_data(data)
            running_intervals = extract_running_intervals(data)
            running_intervals['distance'].plot.density(label=id, ax=ax[0])  
            running_intervals['duration'].plot.density(label=id, ax=ax[1])         
       
        except Exception as e:
            print(str(file) + ' ' + str(e))

    ax[0].legend()
    ax[1].legend()
    plt.show()

    return 

if __name__ == "__main__":
    # get .tcx files
    files = get_activities_list('Activities')
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'last2':
            files = [files[-2], files[-1]]
        elif sys.argv[1] == 'last3':
            files = [files[-3], files[-2], files[-1]]
        elif sys.argv[1] == 'first-last':
            files = [files[0], files[-1]]
        elif sys.argv[1] == 'all':
            files = files
    else:
        files = [files[-1]]

    main(files)