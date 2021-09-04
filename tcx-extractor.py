#%%
from os import walk
import xml.etree.ElementTree as ET
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

def parse_garmin_tcx(filename):
    """ Parses tcx activity file from Garmin Connect to Pandas DataFrame object
    Args: filename (str) - tcx file

    Returns: a tuple of id(str) and data(DataFrame)
    DF columns=['time'(datetime.time), 'distance, m'(float), 'HR'(int), 
    'cadence'(int), 'speed, m/s'(int)]
    """
    tree = ET.parse(filename)
    root = tree.getroot()

    # set namespaces for garmin xct file
    ns = {
        'ns0_training_center_db': 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2',
        'ns3_activity_ext': 'http://www.garmin.com/xmlschemas/ActivityExtension/v2',
    }

    trackpoints = pd.DataFrame(columns=['time', 'distance', 'HR', 'cadence', 'speed'])
    dist, hr, cad, speed = 0, 0, 0, 0
    
    # get data for all trackpoints
    for trackpoint in root.iter('{' + ns['ns0_training_center_db'] + '}' +'Trackpoint'):
        time = trackpoint[0].text
        dist = float(trackpoint[3].text)
        hr = int(trackpoint[4][0].text)
        cad = int(trackpoint[5][0][1].text)*2
        speed = float(trackpoint[5][0][0].text) 
        trackpoints = trackpoints.append(pd.Series({'time': time, 'distance': dist, 
                                                    'HR': hr, 'cadence': cad, 
                                                    'speed': speed}), ignore_index=True)

    trackpoints.loc[:,'time'] = pd.to_datetime(trackpoints['time'])
    id = [x.text for x in root.iter('{' + ns['ns0_training_center_db'] + '}' +'Id')][0]

    return (id, trackpoints)

def clean_garmin_tcx_data(data):
    '''
    Replace zero cadence and speed value, calculate pace,
    transform astronomic time to time from the beginning of activity 

    Args: data(pd.DataFrame) - data from parse_garmin_tcx() function
    Returns: data(pd.DataFrame)
    '''
    #fill missing speed/cadence data (previous not null value)
    data.loc[:,'cadence'].replace(0, method='ffill', inplace=True)
    data.loc[:,'speed'].replace(0, method='ffill', inplace=True)

    #calculate pace [min/km]
    data['pace'] = data['speed'].apply(lambda x: 1 / (x * 0.06))
    data.loc[:, 'time'] = data.loc[:, 'time'] - data.loc[0, 'time']
    data.loc[:, 'time'] = data.loc[:, 'time'].apply(lambda x: x.total_seconds())

    return data

def plot_running_stats(data):
    '''
    Plot pace, cadence, hr vs time from dataframe 
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
#load data
id, data = parse_garmin_tcx('activity_garmin.tcx')
data = clean_garmin_tcx_data(data)
#plot_running_stats(data)


#%%
# separate running from walking intervals
# calculate differences by data points
intervals = data.diff()
intervals.columns = ['dtime', 'ddist', 'dHR', 'dcad', 'dspeed', 'dpace']
intervals.drop(0, inplace=True)
intervals.drop(columns='dspeed', inplace=True)

# find start/stop indices based on cadence increase/decrease level
walkrun_cadence_threshold = 30
started_running = intervals[intervals['dcad'] > walkrun_cadence_threshold].index.values.tolist()
stopped_running = intervals[intervals['dcad'] < -walkrun_cadence_threshold].index.values.tolist()

# filter and save intervals data from dataset to separate dataframe
running_intervals = pd.DataFrame(columns=['start time', 'stop time', 'start dist', 
                                        'stop dist'])
for start, stop in zip(started_running, stopped_running):
    start_time = data.loc[start, 'time']
    stop_time = data.loc[stop, 'time']
    start_dist = data.loc[start, 'distance']
    stop_dist = data.loc[stop, 'distance']
    running_intervals = running_intervals.append(pd.Series({'start time': start_time, 
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

# running_intervals.plot(x='start time', y='distance')
# running_intervals.plot(x='start time', y='duration')

# calculate hr variability (increase/decrease rate)
hrv_rate = intervals['dHR'] / intervals['dtime']
hr_time = data.iloc[:-1]['time'] + (intervals['dtime'].reset_index(drop=True) / 2)

#%%

def main():
    pass

if __name__ == "__main__":
    pass