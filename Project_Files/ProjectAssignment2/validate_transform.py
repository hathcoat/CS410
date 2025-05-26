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

    def _long_exist(self, record):
        try:
            assert record["GPS_LONGITUDE"] != None
            return True

        except Exception:
            return False

    def _lat_exist(self, record):
        try:
            assert record["GPS_LATITUDE"] != None
            return True

        except Exception:
            return False

    def _meters_exist(self, record):
        try:
            assert record["METERS"] != None
            return True

        except Exception:
            return False

    def _time_exist(self, record):
        try:
            assert record["ACT_TIME"] != None
            return True

        except Exception:
            return False

    def _long_in_range(self, record):
        try:
            assert record["GPS_LONGITUDE"] > -124 and record["GPS_LONGITUDE"] < -121
            return True

        except Exception:
            return False

    def _lat_in_range(self, record):
        try:
            assert record["GPS_LATITUDE"] > 44 and record["GPS_LATITUDE"] < 47
            return True

        except Exception:
            return False

    def _time_in_range(self, record):
        try:
            assert record["ACT_TIME"] > 0
            return True

        except Exception:
            return False

    def _meters_in_range(self, record):
        try:
            assert record["METERS"] > 0
            return True
        except Exception:
            return False

    def _first_validate(self, record):
        return self._long_exist(record) and self._lat_exist(record) and self._meters_exist(record) and self._time_exist(record) and self._long_in_range(record) and self._lat_in_range(record) and self._time_in_range(record) and self._meters_in_range(record)

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
                if first and self._speed_exists(self.data[j]) and self._speed_in_range(self.data[j]):
                    good_records.append(self.data[j])
                    first = False

                elif self._second_validate(self.data[j-1],  self.data[j]):
                    good_records.append(self.data[j])
                j += 1
    
        self.data = good_records

    def _second_validate(self, prev_rec, rec):
        return self._speed_exists and self._meters_nondecreasing(prev_rec, rec) and self._travel_expected(prev_rec, rec) and self._speed_in_range(rec)

    def _speed_exists(self, record):
        try:
            assert record["SPEED"] != None
            return True

        except Exception:
            return False
    def _meters_nondecreasing(self, prev_record, record):
        try:
            assert prev_record["METERS"] < record["METERS"]
            return True
        except Exception:
            return False

    def _travel_expected(self, prev_record, record):
        try:
            assert record["METERS"] - prev_record["METERS"] < 1000
            return True
        except Exception:
            return False

    def _speed_in_range(self, record):
        try:
            assert record["SPEED"] > 0 and record["SPEED"] < 38
            return True
        except Exception:
            return False

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
