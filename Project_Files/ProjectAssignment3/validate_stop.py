from collections import defaultdict
import datetime

class validate_stop():
    def __init__(self, data):
        self.data = data

    def check_data(self):
        good_recs = []
        #This prints a number of records.
        for r in self.data:
            if self._check_trip(r) and self._check_route(r) and self._check_vehicle(r) and self._check_service(r) and self._check_direction(r):
                good_recs.append(r)

        self.data = good_recs

    def _check_trip(self, rec):
        try:
            assert rec["trip_number"] != None
            assert rec["trip_number"].strip() != ""
            return True

        except Exception:
            return False


    def _check_route(self, rec):
        try:
            assert rec["route_number"] != None
            assert rec["route_number"].strip() != ""
            return True
        except Exception:
            return False
            

    def _check_vehicle(self, rec):
        try:
            assert rec["vehicle_number"] != None
            assert rec["vehicle_number"].strip() != ""
            return True
        except Exception:
            return False

    def _check_service(self, rec):
        try:
            assert rec["service_key"] != None
            assert rec["service_key"].strip() != ""
            assert self._transform_service_key(rec) == True
            return True
        except Exception:
            return False

    def _check_direction(self, rec):
        try:
            assert rec["direction"] != None
            assert rec["direction"].strip() != ""
            assert self._transform_direction(rec) == True
            return True
        except Exception:
            return False

    def _transform_service_key(self, rec):
        if rec["service_key"] == "W" or rec["service_key"] == "M":
            rec["service_key"] = "Weekday"

        elif rec["service_key"] == "S":
            rec["service_key"] = "Saturday"

        elif rec["service_key"] == "U":
            rec["service_key"] = "Sunday"
        else:
            return False
        return True

    def _transform_direction(self, rec):
        if rec["direction"] == "0":
            rec["direction"] = "Out"

        elif rec["direction"] == "1":
            rec["direction"] = "Back"

        else:
            return False

        return True

    def update_recs(self, data):
        self.data = data

    def get_data(self):
      return self.data

    def get_unique_trip_recs(self):
        seen = set()
        unique_recs = []

        for record in self.data:
            record_tuple = tuple(record.items())
            if record_tuple not in seen:
                seen.add(record_tuple)
                unique_recs.append(record)

        return unique_recs
