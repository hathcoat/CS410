#Cody Hathcoat      CS410
from collections import defaultdict
import datetime

class validate_transform:
    def __init__(self, data):
        self.data = data

    def first_validate(self):
        good_records = []

        for record in self.data:
            if self._first_validate(record):
                good_records.append(record)

        self.data = good_records
        self._order_trip()

    def _first_validate(self, record):
        #Assertion 1: The GPS_LONGITUDE must exist.
        if record["GPS_LONGITUDE"] is None:
            return False

        #Assertion 2: The GPS_LATITUDE must exist.
        if record["GPS_LATITUDE"] is None:
            return False 

        #Assertion 3: The METERS must exist.
        if record["METERS"] is None:
            return False

        #Assertion 4:  The ACT_TIME must exit.
        if record["ACT_TIME"] is None:
            return False

        #Assertion 5: The GPS_LATITUDE must be between 44.0 and 47.0.
        if record["GPS_LATITUDE"] < 44.0 or record["GPS_LATITUDE"] > 47.0:
            return False

        #Assertion 6: The GPS_LONGITUDE must be between -124.0 and -121.0.
        if record["GPS_LONGITUDE"] < -124.0 or record["GPS_LONGITUDE"] > -121.0:
            return False
       
        #Assertion 7: The ACT_TIME can't be less than 0.
        if record["ACT_TIME"] < 0:
            return False
        
        #Assertion 8: The METERS can't be less than 0.
        if record["METERS"] < 0:
            return False

        return True

    #This function should handle sorting by TRIP_ID and then sort each sub TRIP_ID list by act time
    #to be called after first_validate
    def _order_trip(self):
        trips = defaultdict(list)
        for record in self.data:
            trips[record["EVENT_NO_TRIP"]].append(record)

        self.data =  []
        for trip_id, trip_records in trips.items():
            trip_records.sort(key=lambda r: r["ACT_TIME"])
            self.data.extend(trip_records)

    def second_validate(self):
        good_records = []
        current_trip = 0
        j = 0

        # You have multiple trips, can't just loop through, need to sepearte out by trips, loop below is wrong.
        while j < len(self.data):
            current_trip = self.data[j]["EVENT_NO_TRIP"]
            first = True
            while j < len(self.data) and self.data[j]["EVENT_NO_TRIP"] == current_trip:
                if first and self.data[j]["SPEED"] is not None and self.data[j]["SPEED"] > 0 and self.data[j]["SPEED"] < 44.704:
                    good_records.append(self.data[j])
                    first = False

                elif self._second_validate(self.data[j-1],  self.data[j]):
                    good_records.append(self.data[j])
                j += 1
    
        self.data = good_records

    def _second_validate(self, prev_rec, rec):
        # Assertion 9: A SPEED value must exist for a record.
        if rec["SPEED"] is None:
            return False

        #Assertion 10: A trips METERS must be nondecreasing.
        if rec["METERS"] < prev_rec["METERS"]:
            return False

        #Assertion 11: A bus should not move more then 250 meters between consecutive records on the same trip.
        if rec["METERS"] - prev_rec["METERS"] > 1000:
            return False

        #Assertion 12: A SPEED value can't be less than 0 or greater then 38 m/s
        if rec["SPEED"] < 0 or rec["SPEED"] > 38:
            return False

        return True

    def transform_data(self):
        j = 0
        while j < len(self.data):
            current_trip = self.data[j]["EVENT_NO_TRIP"]
            first_index = j
            while j < len(self.data)and self.data[j]["EVENT_NO_TRIP"] == current_trip:
                if j > first_index:
                    self._add_speed(self.data[j-1], self.data[j])
                else:
                    self.data[j]["SPEED"] = None

                self._add_tstamp(self.data[j])
                j += 1
            if first_index + 1 < j:
                self.data[first_index]["SPEED"] = self.data[first_index+1]["SPEED"]

    def _add_speed(self, prev_rec, rec):
        dt = rec["ACT_TIME"] - prev_rec["ACT_TIME"]
        dx = rec["METERS"] - prev_rec["METERS"]

        #Assertion 13: Two bus records on the same trip should not share the same act_time value
        if dt > 0: #Time interval is as expected.
            speed = round(dx / dt, 2)
            rec["SPEED"] = speed

        else:
            rec["SPEED"] = None

    def _add_tstamp(self, rec):
        opd = datetime.datetime.strptime(rec["OPD_DATE"], "%d%b%Y:%H:%M:%S")
        seconds = datetime.timedelta(seconds=rec["ACT_TIME"])
        timestamp = opd + seconds
        rec["TIMESTAMP"] = timestamp.isoformat()

    def get_data(self):
        return self.data
