#%%
import xml.etree.ElementTree as ET
from numpy import append
import pandas as pd
from pandas.core.series import Series

tree = ET.parse('activity_garmin.tcx')
root = tree.getroot()

# set namespaces for garmin xct file
ns = {
  'ns0_training_center_db': 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2',
  'ns2_user_profile': 'http://www.garmin.com/xmlschemas/UserProfile/v2',
  'ns3_activity_ext': 'http://www.garmin.com/xmlschemas/ActivityExtension/v2',
  'ns4_profile_ext': 'http://www.garmin.com/xmlschemas/ProfileExtension/v1',
  'ns5_goals': 'http://www.garmin.com/xmlschemas/ActivityGoals/v1'
}

trackpoints = pd.DataFrame(columns=['distance', 'HR', 'cadence', 'speed'])

dist, hr, cad, speed = 0, 0, 0, 0
# get data for all trackpoints
for trackpoint in root.iter('{' + ns['ns0_training_center_db'] + '}' +'Trackpoint'):
    dist = round(float(trackpoint[3].text),3)
    hr = int(trackpoint[4][0].text)
    cad = int(trackpoint[5][0][1].text)*2
    speed = float(trackpoint[5][0][0].text) #calc pace: round(1 / (float(trackpoint[5][0][0].text) * 0.06))
    trackpoints = trackpoints.append(pd.Series({'distance': dist, 'HR': hr, 'cadence': cad, 'speed': speed}), 
                                                                      ignore_index=True)


