from config import *
from google.oauth2 import service_account
import googleapiclient.discovery
import pandas as pd
from datetime import datetime

SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
SERVICE_ACCOUNT_FILE = PATH_TO_SERVICE_ACCOUNT_JSON

start_time =  datetime.strptime('2018-01-01T00:00:00', "%Y-%m-%dT%H:%M:%S").isoformat() + 'Z'
end_time   =  datetime.utcnow().isoformat() + 'Z'

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
cal = googleapiclient.discovery.build(
        'calendar', 'v3', credentials=credentials)

events_df_all = pd.DataFrame()
page_token = None
while True:
  events_result = cal.events().list(
                            calendarId=CALENDAR_ID, timeMin = start_time,
                            timeMax = end_time, pageToken=page_token,
                            singleEvents=True,maxResults=250,
                            orderBy='startTime').execute()
  events = events_result.get('items', [])
  events_df_page = pd.io.json.json_normalize(events)
  events_df_all = events_df_all.append(events_df_page)
  page_token = events_result.get('nextPageToken')
  if not page_token:
    break



events_filtered = events_df_all[['attendees','creator.email','end.dateTime',
                                    'end.timeZone','htmlLink',
                                    'iCalUID','organizer.email',
                                    'recurringEventId','start.dateTime',
                                    'start.timeZone','summary'
                                   ]].copy()

events_filtered[['start.dateTime','end.dateTime']] = events_filtered[['start.dateTime','end.dateTime']].apply(pd.to_datetime, errors='ignore')

events_filtered[['start.timeZone','end.timeZone']] = events_filtered[['start.timeZone','end.timeZone']].fillna(method='ffill')

events_filtered.dropna(subset = ['start.dateTime', 'end.dateTime'],inplace = True)



def localize_ts(row):
     return row['start.dateTime'].tz_localize(row['start.timeZone']),row['end.dateTime'].tz_localize(row['end.timeZone'])

events_filtered[['start.dateTime','end.dateTime']]= events_filtered.apply(localize_ts, axis=1, result_type="expand")
events_filtered['duration'] = (events_filtered['end.dateTime'] - events_filtered['start.dateTime']).astype('timedelta64[m]').astype(int)

events_filtered.columns = events_filtered.columns.str.replace(".", "_")



from config import *

schema = [
    {'name': 'attendees', 'type': 'STRING'},
    {'name': 'creator_email', 'type': 'STRING'},
    {'name': 'end_dateTime', 'type': 'TIMESTAMP'},
    {'name': 'end_timeZone', 'type': 'STRING'},
    {'name': 'htmlLink', 'type': 'STRING'},
    {'name': 'iCalUID', 'type': 'STRING'},
    {'name': 'organizer_email', 'type': 'STRING'},
    {'name': 'recurringEventId', 'type': 'STRING'},
    {'name': 'start_dateTime', 'type': 'TIMESTAMP'},
    {'name': 'start_timeZone', 'type': 'STRING'},
    {'name': 'summary', 'type': 'STRING'},
    {'name': 'duration', 'type': 'INTEGER'}
]

events_filtered.to_gbq(destination_table=BQ_TABLE_NAME,
                       project_id=GOOGLE_CLOUD_PROJECT_ID,if_exists = 'replace',
                      private_key=PATH_TO_SERVICE_ACCOUNT_JSON
                        , table_schema = schema
                      )
