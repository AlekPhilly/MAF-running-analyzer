import sys
from lxml import etree
from pandas import DataFrame, Series, to_datetime
import datetime as dt
import matplotlib.pyplot as plt
from pathlib import Path
from numpy import nan, inf
import seaborn as sns
from time import perf_counter
from db_interaction import init_db, add_activity_id, add_data
from db_interaction import truncate_db, drop_all, processed_files
from db_interaction import fetchone, fetchmany, processed_activities

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
    walk_cadence = min([x for x in cadence if x < avg_cadence], key = lambda x: avg_cadence - x)

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
    if len(started_running) != len(stopped_running):
        print(f'{len(started_walking)} start points\n{len(stopped_walking)} stop points')
        raise ValueError("Quanties of start and stop points aren't equal (walking)")

    # filter and save intervals data from dataset to separate dataframe
    running_intervals = DataFrame(columns=['start_time', 'stop_time', 'start_dist', 
                                            'stop_dist'])
    
    for start, stop in zip(started_running, stopped_running):
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


def update_db(db_name, username, password, activities_dir):
    
    activities = get_activities_list(activities_dir)
    processed_activities = processed_files(db_name, username, password)

    for activity in activities:

        if activity.name in processed_activities:
            print('Entry already exists')
            continue
        
        try:
            id, data = parse_garmin_tcx(activity)
            data = clean_garmin_tcx_data(data)
            intervals = extract_running_intervals(data)
        except:
            continue

        intervals['act_id'] = id
        data['act_id'] = id
        data.columns = [x.lower() for x in data.columns]
        intervals.columns = [x.lower() for x in intervals.columns]

        run_duration = intervals[intervals['type'] == 'run']['duration'].sum()
        walk_duration = intervals[intervals['type'] == 'walk']['duration'].sum()
        run_pct = round(run_duration / (run_duration + walk_duration) * 100, 1)

        activity_stats = {'act_id': id, 'duration': str(dt.timedelta(seconds=data['time'].values[-1])),
                        'distance': round(data['distance'].values[-1] / 1000, 1), 
                        'pace': round(data[data['pace'] != inf]['pace'].mean(), 1),
                        'avg_hr': round(data['hr'].mean()),
                        'run_pct': run_pct}
        summary = DataFrame([activity_stats])

        try:
            add_activity_id(db_name, username, password, id, activity.name)
        except:
            print('Entry already exists')
            continue

        for table, dta in zip(('trackpoints', 'intervals', 'summary'), 
                                (data, intervals, summary)):
            add_data(db_name, username, password, table, dta)

    return


def plot_box(quantity):
    counter = perf_counter()
    
    db_name, username, password = Path('db_credentials.txt').read_text().splitlines()
    
    files_indb = set(processed_files(db_name, username, password))
    files_indir = get_activities_list('Activities')
    new_activities = {x.name for x in files_indir} - files_indb
    
    if new_activities:
        update_db(db_name, username, password, 'Activities')

    act_ids = processed_activities(db_name, username, password)[-quantity:]

    if len(act_ids) > 1:
        act_ids = tuple(str(x) for x in act_ids)
        _, intervals, summary = fetchmany(db_name, username, password, act_ids)
    else:
        act_ids = act_ids[0]
        _, intervals, summary = fetchone(db_name, username, password, act_ids)

    summary.loc[:, 'act_id'] = summary['act_id'].dt.strftime('%Y-%m-%d')
    intervals.loc[:, 'act_id'] = intervals['act_id'].dt.strftime('%Y-%m-%d')

    counter = perf_counter() - counter
    print(f'preparing data took {counter:.3f} s')

    counter = perf_counter()
    plt.close('all')
    fig, ax  = plt.subplots(nrows=2, ncols=2, figsize=(10, 5))
    fig.autofmt_xdate()
    # ax[0, 0].title.set_text('Distance, m')
    # ax[0, 1].title.set_text('Duration, s')
    # ax[1, 0].title.set_text('HR var rate, bpm')
    # ax[1, 1].title.set_text('Summary')

    dist_plot = sns.boxplot(x='act_id', y='distance', data=intervals, hue='type', 
                showfliers=False, ax=ax[0, 0])
    duration_plot = sns.boxplot(x='act_id', y='duration', data=intervals, hue='type', 
                showfliers=False, ax=ax[0, 1])
    hr_plot = sns.violinplot(x='act_id', y='hrrate', data=intervals, hue='type', 
                showfliers=False, split=True, ax=ax[1, 0])
    for plot in [dist_plot, duration_plot, hr_plot]:
        plot.set_xlabel(None)
        plot.grid(axis='y', alpha=0.5)
    ax[0, 1].get_legend().remove()
    ax[1, 0].get_legend().remove()
    
    cellText = [text for _, text in summary.iterrows()] 
    ax[1, 1].axis('off')
    table = ax[1, 1].table(cellText=cellText, colLabels=summary.columns, 
                    cellLoc='center', loc='best')
    # table.auto_set_font_size(False)
    # table.set_fontsize(6)
    counter = perf_counter() - counter
    print(f'plotting took {counter:.3f} s')
    plt.show()

    return


def plot_density(quantity):
    den_cnt = perf_counter()
    db_name, username, password = Path('db_credentials.txt').read_text().splitlines()
    
    files_indb = set(processed_files(db_name, username, password))
    files_indir = get_activities_list('Activities')
    new_activities = {x.name for x in files_indir} - files_indb
    
    if new_activities:
        update_db(db_name, username, password, 'Activities')

    act_ids = processed_activities(db_name, username, password)[-quantity:]

    # prepare canvas
    plt.close('all')
    fig, ax  = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))
    ax[0].title.set_text('Distance, m')
    ax[1].title.set_text('Duration, s')
    
    for act_id in act_ids:
        _, running_intervals, _ = fetchone(db_name, username, password, act_id)

        running_intervals['distance'].plot.density(label=act_id, ax=ax[0])  
        running_intervals['duration'].plot.density(label=act_id, ax=ax[1])         

    ax[0].legend()
    ax[1].legend()
    den_cnt = perf_counter() - den_cnt
    print(f'running took {den_cnt} s')
    plt.show()

    return 



#%%
def main(quantity, mode='box'):

    if mode == 'kde':
        plot_density(quantity)
    elif mode == 'box':
        plot_box(quantity)
    else:
        raise ValueError('Unknown mode')

    return


if __name__ == "__main__":
   
    if len(sys.argv) > 1:
        quantity = int(sys.argv[1])
    else:
        quantity = 1

    if len(sys.argv) == 3:
        mode = sys.argv[2]

    main(quantity, mode)