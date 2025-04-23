import pandas as pd
import numpy as np


df = pd.read_csv("bc_trip259172515_230215.csv")
count = 0
for index, row in df.iterrows():
    count += 1
print(f"{count} records")
#print(df)

# No EVENT_NO_STOP
df2 = df.drop(columns=['EVENT_NO_STOP'])
#print(df2)

df3 = df2.drop(columns=["GPS_SATELLITES"])
#print(df3)

df4 = pd.read_csv("bc_trip259172515_230215.csv", usecols = ['EVENT_NO_TRIP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_HDOP'])
# Insert a blank timestamp column.
df4['TIMESTAMP'] = np.nan


def handle_timestamp(row):
        day_time = pd.to_datetime(row["OPD_DATE"],  format="%d%b%Y:%H:%M:%S")
        time_time = pd.to_timedelta(row["ACT_TIME"], unit="s")
        print(f"{day_time}, {time_time}")
        return day_time + time_time

#Axis = 1 means pass each row, not column.
df4["TIMESTAMP"] = df4.apply(handle_timestamp, axis=1)
#print(df4)

df4 = df4.drop(columns=["OPD_DATE", "ACT_TIME"])
#print(df4)

df4["dMETERS"] = df4["METERS"].diff() #Computes the difference between each row's METERS and the one before it.
df4["dTIMESTAMP"] = df4["TIMESTAMP"].diff().dt.total_seconds() # dt.total_seconds() converts the time difference into seconds

df4["SPEED"] = df4.apply(
      # Calculates speed if meters is not null and timestamp is not 0
      lambda row: row["dMETERS"] / row["dTIMESTAMP"] if pd.notnull(row["dMETERS"]) and row["dTIMESTAMP"] > 0 else 0,
      axis=1
)

if len(df4) > 1:
      # df.loc[row_label, column_label] to access/modify table.
      m0 = df4.loc[0, "METERS"]
      m1 = df4.loc[1, "METERS"]
      t0 = df4.loc[0, "TIMESTAMP"]
      t1 = df4.loc[1, "TIMESTAMP"]

      dm = m1 - m0
      dt = (t1 - t0).total_seconds()

      df4.loc[0, "SPEED"] = dm / dt if dt > 0 else 0


df4 = df4.drop(columns=["dMETERS", "dTIMESTAMP"])

print(df4)

print("Min speed:", df4["SPEED"].min())
print("Max speed:", df4["SPEED"].max())
print("Average speed:", df4["SPEED"].mean())

print(df4.loc[df4["SPEED"].idxmin()])






