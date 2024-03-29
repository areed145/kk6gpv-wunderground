import urllib.request
from pymongo import MongoClient
import pandas as pd
import os
import json
from datetime import date, datetime, timedelta

from pymongo.common import _MAX_END_SESSIONS


class Wunderground:
    """Class that fetches weather from Wunderground and copies to MongoDB"""

    def __init__(self, dbc, sid, api):
        """
        Initializes class
        """
        self.dbc = dbc
        self.sid = sid
        self.api = api

    def convert(self, val):
        """
        Convert values to float
        """
        try:
            val = float(val)
        except Exception:
            pass
        if val == " ":
            val = None
        return val

    def get_day(self, m, d, y):
        """
        Get observations for the full day
        """
        url = (
            "https://api.weather.com/v2/pws/history/all?stationId="
            + str(self.sid)
            + "&format=json&units=e&apiKey="
            + str(self.api)
            + "&date="
            + str(y).zfill(4)
            + str(m).zfill(2)
            + str(d).zfill(2)
        )
        print(url)
        hdr = {
            "User-Agent": "Mozilla/5.0 (Macintosh; \
                Intel Mac OS X 10_10_1) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/39.0.2171.95 \
                Safari/537.36"
        }
        req = urllib.request.Request(url, headers=hdr)
        msg = urllib.request.urlopen(req).read()
        msg = json.loads(msg)
        # print(msg)
        obs = msg["observations"]
        for ob in obs:
            ob["station_id"] = ob["stationID"]
            ob.pop("stationID")
            ob["obs_time_utc"] = pd.to_datetime(ob["obsTimeUtc"])
            ob.pop("obsTimeUtc")
            ob["obs_time_local"] = pd.to_datetime(ob["obsTimeLocal"])
            ob.pop("obsTimeLocal")
            ob.update(ob["imperial"])
            ob.pop("imperial")
            ob["solar"] = ob["solarRadiationHigh"]
            ob.pop("solarRadiationHigh")
            ob["uv"] = ob["uvHigh"]
            ob.pop("uvHigh")
            ob["wind_deg"] = ob["winddirAvg"]
            ob.pop("winddirAvg")
            ob["temp_f"] = ob["tempAvg"]
            ob.pop("tempAvg")
            ob.pop("tempHigh")
            ob.pop("tempLow")
            ob["dewpt_f"] = ob["dewptAvg"]
            ob.pop("dewptAvg")
            ob.pop("dewptHigh")
            ob.pop("dewptLow")
            ob["humidity"] = ob["humidityAvg"]
            ob.pop("humidityAvg")
            ob.pop("humidityHigh")
            ob.pop("humidityLow")
            ob["pressure_in"] = (ob["pressureMax"] + ob["pressureMin"]) / 2
            ob.pop("pressureMax")
            ob.pop("pressureMin")
            ob["pressure_trend"] = ob["pressureTrend"]
            ob.pop("pressureTrend")
            ob["qc_status"] = ob["qcStatus"]
            ob.pop("qcStatus")
            ob["heat_index_f"] = ob["heatindexAvg"]
            ob.pop("heatindexAvg")
            ob.pop("heatindexHigh")
            ob.pop("heatindexLow")
            ob["windchill_f"] = ob["windchillAvg"]
            ob.pop("windchillAvg")
            ob.pop("windchillHigh")
            ob.pop("windchillLow")
            ob["wind_speed_mph"] = ob["windspeedAvg"]
            ob.pop("windspeedAvg")
            ob.pop("windspeedHigh")
            ob.pop("windspeedLow")
            ob["wind_gust_mph"] = ob["windgustAvg"]
            ob.pop("windgustAvg")
            ob.pop("windgustHigh")
            ob.pop("windgustLow")
            ob["precip_rate"] = ob["precipRate"]
            ob.pop("precipRate")
            ob["precip_total"] = ob["precipTotal"]
            ob.pop("precipTotal")
            ob.pop("epoch")
            ob.pop("tz")
            try:
                self.raw.insert_one(ob)
                print(ob)
            except Exception:
                print("duplicate observation")

    def run(self):
        """
        Main method to run
        """
        client = MongoClient(self.dbc)
        db = client.wx
        self.raw = db.raw

        y_s = 2022
        m_s = 1
        d_s = 31
        y_e = 2022
        m_e = 2
        d_e = 1
        datetime_ = datetime(year=y_s, month=m_s, day=d_s)
        datetime_end = datetime(year=y_e, month=m_e, day=d_e)
        while datetime_ <= datetime_end:
            y = datetime_.year
            m = datetime_.month
            d = datetime_.day
            try:
                self.get_day(m, d, y)
                print("got day {}-{}-{}".format(y, m, d))
            except Exception:
                print("failed day {}-{}-{}".format(y, m, d))
                pass
            datetime_ += timedelta(days=1)


if __name__ == "__main__":
    wunderground = Wunderground(
        dbc=os.environ["MONGODB_CLIENT"],
        sid=os.environ["SID"],
        api=os.environ["API"],
    )
    wunderground.run()
