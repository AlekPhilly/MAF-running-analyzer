from garminconnect import Garmin
import datetime
import logging
import time

def scrape_activities(login, password, start_date, output_dir):

    api = Garmin(login, password)
    api.login()

    activities = api.get_activities_by_date(start_date, 
                                            datetime.date.today().isoformat(), 'running')

    counter = 0
    for activity in activities:
        activity_id = activity['activityId']

        tcx_data = api.download_activity(activity_id, dl_fmt=api.ActivityDownloadFormat.TCX)
        output_file = f'./{output_dir}/activity_{str(activity_id)}.tcx'
        with open(output_file, 'wb') as fb:
            fb.write(tcx_data)
        counter += 1

    logging.info('added %s activities from garmin.com', counter)

    return counter

def download_new_tcx(login, password, start_date, output_dir):
    new_act = 0
    while new_act == 0:
        try:
            new_act = scrape_activities(login, password, start_date, output_dir)
        except Exception as e:
            logging.error(e)
            time.sleep(5)