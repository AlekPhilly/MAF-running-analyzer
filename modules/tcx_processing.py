from lxml import etree
from matplotlib.pyplot import contour
from pandas import DataFrame, Series, to_datetime
from numpy import nan, inf


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

    id = to_datetime(tree.find('.//' + ns['ns0'] + 'Id').text)

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
    # fill not recorded or missing data (previous not null value)
    data[['speed', 'cadence', 'latitude', 'longitude', 'altitude']] = data[['speed', 'cadence', 'latitude', 'longitude', 'altitude']].replace(0, method='ffill')
    data.fillna(method='ffill', inplace=True)

    data.loc[:, 'pace'] = round(1 / (0.06 * data.loc[:, 'speed']), 2)
    data.loc[:, 'pace'] = data['pace'].replace(inf, nan)

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
    # calculate walking cadence (avg cadence for brisk walk = 100)
    cadence = sorted([x for x in data['cadence'].values if x > 80])
    avg_cadence = round((min(cadence) + max(cadence)) / 2, 0)
    walk_cadence = min([x for x in cadence if 0 < x < avg_cadence], key = lambda x: avg_cadence - x)

    # separate running from walking intervals
    started_running, stopped_running = [], []
    started_walking, stopped_walking = [0], []
    for n in range(1, len(data.index)):
        if data.loc[n, 'cadence'] > walk_cadence and data.loc[n - 1, 'cadence'] <= walk_cadence:
            started_running.append(n)
            stopped_walking.append(n - 1)
        if data.loc[n, 'cadence'] <= walk_cadence and data.loc[n - 1, 'cadence'] > walk_cadence:
            stopped_running.append(n - 1)
            started_walking.append(n)
    stopped_walking.append(data.index[-1])

    if len(started_running) != len(stopped_running):
        print(f'{len(started_running)} start points\n{len(stopped_running)} stop points')
        raise ValueError("Quanties of start and stop points aren't equal (running)")
    if len(started_walking) != len(stopped_walking):
        print(f'{len(started_walking)} start points\n{len(stopped_walking)} stop points')
        raise ValueError("Quanties of start and stop points aren't equal (walking)")

    # filter and save intervals data from dataset to separate dataframe
    running_intervals = DataFrame(columns=['start_time', 'stop_time', 'start_dist', 
                                            'stop_dist'])
    
    for start, stop in zip(started_running, stopped_running):
        # filter error in points determination
        if start == stop:
            continue
        start_time = data.loc[start, 'time']
        start_dist = data.loc[start, 'distance']
        stop_time = data.loc[stop, 'time']
        stop_dist = data.loc[stop, 'distance']
        dHR = data.loc[stop, 'HR'] - data.loc[start, 'HR']
        
        running_intervals = running_intervals.append(Series({'start_time': start_time, 
                                                    'stop_time': stop_time,'start_dist': start_dist, 
                                                    'stop_dist': stop_dist, 'dHR': dHR,
                                                    'type': 'run'}), ignore_index=True)
    
    for start, stop in zip(started_walking, stopped_walking):
        # filter error in points determination
        if start == stop:
            continue
        start_time = data.loc[start, 'time']
        start_dist = data.loc[start, 'distance']
        stop_time = data.loc[stop, 'time']
        stop_dist = data.loc[stop, 'distance']
        dHR = data.loc[stop, 'HR'] - data.loc[start, 'HR']
        
        running_intervals = running_intervals.append(Series({'start_time': start_time, 
                                                    'stop_time': stop_time,'start_dist': start_dist, 
                                                    'stop_dist': stop_dist, 'dHR': dHR,
                                                    'type': 'walk'}), ignore_index=True)
    
    # calculate intervals duration and distance covered
    running_intervals['duration'] = running_intervals['stop_time'] - running_intervals['start_time']
    running_intervals['distance'] = running_intervals['stop_dist'] - running_intervals['start_dist']

    running_intervals['HRrate'] = abs(running_intervals['dHR'] / running_intervals['duration'] * 60)

    # convert time from seconds to hh:mm:ss format
    # running_intervals.loc[:, 'start time'] = running_intervals.loc[:, 'start time'].apply(
    #                                             lambda x: str(dt.timedelta(seconds=x)))
    # running_intervals.loc[:, 'stop time'] = running_intervals.loc[:, 'stop time'].apply(
    #                                             lambda x: str(dt.timedelta(seconds=x)))

    return running_intervals

