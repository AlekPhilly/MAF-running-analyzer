import sys
import datetime as dt
import time
import matplotlib.pyplot as plt
from pathlib import Path
from numpy import nan, inf
import seaborn as sns
from time import perf_counter
from modules.classes import GarminDB
from modules.utilities import timer
from modules.scraping import download_new_tcx
import logging


logging.basicConfig(format='%(levelname)s | %(asctime)s | %(message)s', 
                    datefmt='%y-%m-%d %H:%M:%S', level=logging.INFO)

ACTIVITIES_DIR = 'Activities'
DB_NAME, DB_USERNAME, DB_PASSWORD = Path('db_credentials.txt').read_text().splitlines()
GR_LOGIN, GR_PASSWORD = Path('garmin_credentials.txt').read_text().splitlines()
START_DATE = dt.date.today() - dt.timedelta(days=14) #'2021-08-17'


def plot_box(database, quantity):
    counter = perf_counter() 

    act_ids = database.processed_activities[-quantity:]

    if len(act_ids) > 1:
        act_ids = tuple(str(x) for x in act_ids)
        _, intervals, summary = database.fetchmany(act_ids)
    else:
        act_ids = act_ids[0]
        _, intervals, summary = database.fetchone(act_ids)


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


def plot_density(database, quantity):

    den_cnt = perf_counter()
    
    act_ids = database.processed_activities[-quantity:]

    # prepare canvas
    plt.close('all')
    fig, ax  = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))
    ax[0].title.set_text('Distance, m')
    ax[1].title.set_text('Duration, s')
    
    for act_id in act_ids:
        _, running_intervals, _ = database.fetchone(act_id)

        running_intervals['distance'].plot.density(label=act_id.strftime('%Y-%m-%d'), ax=ax[0])  
        running_intervals['duration'].plot.density(label=act_id.strftime('%Y-%m-%d'), ax=ax[1])         

    ax[0].legend()
    ax[1].legend()
    den_cnt = perf_counter() - den_cnt
    print(f'running took {den_cnt} s')
    plt.show()

    return 



#%%
def main(quantity, mode='box', update=False):
    global DB_NAME, DB_USERNAME, DB_PASSWORD
    global START_DATE, GR_LOGIN, GR_PASSWORD
    global ACTIVITIES_DIR

    if update == True:
        download_new_tcx(GR_LOGIN, GR_PASSWORD, START_DATE, ACTIVITIES_DIR)
  
    database = GarminDB(DB_NAME, DB_USERNAME, DB_PASSWORD)
    database.connect()
    
    database.actualize_db(ACTIVITIES_DIR)

    if mode == 'kde':
        plot_density(database, quantity)
    elif mode == 'box':
        plot_box(database, quantity)
    else:
        database.disconnect()
        raise ValueError('Unknown mode')

    database.disconnect()

    return


if __name__ == "__main__":
   
    quantity = 1
    mode = 'box'
    update = False

    if len(sys.argv) > 1:
        quantity = int(sys.argv[1])
        if sys.argv[2] in ('kde', 'box'):
            mode = sys.argv[2]
        if 'update' in sys.argv:
            update = True
    
    main(quantity, mode, update)