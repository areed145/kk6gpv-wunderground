import urllib.request
from datetime import datetime, timezone
from pymongo import MongoClient
import pandas as pd
import time
import os
import json
import logging

logging.basicConfig()


class Wunderground:
    """Class that fetches weather from Wunderground and copies to MongoDB"""

    def __init__(self, dbc, sid, api):
        """
        Initializes class
        """
        self.dbc = dbc
        self.sid = sid
        self.api = api
        self.logger = logging.getLogger("kk6gpv-wunderground")
        self.logger.setLevel(logging.DEBUG)

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

    def get_current(self):
        """
        Get current observation
        """
        url = (
            "https://api.weather.com/v2/pws/observations/current?stationId="
            + str(self.sid)
            + "&format=json&units=e&apiKey="
            + str(self.api)
        )
        hdr = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 \
            Safari/537.36"
        }
        req = urllib.request.Request(url, headers=hdr)
        msg = urllib.request.urlopen(req).read()
        msg = json.loads(msg)
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
            ob["solar"] = ob["solarRadiation"]
            ob.pop("solarRadiation")
            ob["wind_deg"] = ob["winddir"]
            ob.pop("winddir")
            ob["temp_f"] = ob["temp"]
            ob.pop("temp")
            ob["dewpt_f"] = ob["dewpt"]
            ob.pop("dewpt")
            ob["pressure_in"] = ob["pressure"]
            ob.pop("pressure")
            ob["qc_status"] = ob["qcStatus"]
            ob.pop("qcStatus")
            ob["heat_index_f"] = ob["heatIndex"]
            ob.pop("heatIndex")
            ob["windchill_f"] = ob["windChill"]
            ob.pop("windChill")
            ob["wind_speed_mph"] = ob["windSpeed"]
            ob.pop("windSpeed")
            ob["wind_gust_mph"] = ob["windGust"]
            ob.pop("windGust")
            ob["precip_rate"] = ob["precipRate"]
            ob.pop("precipRate")
            ob["precip_total"] = ob["precipTotal"]
            ob.pop("precipTotal")
            ob["station_type"] = ob["softwareType"]
            ob.pop("softwareType")
            ob["realtime_freq"] = ob["realtimeFrequency"]
            ob.pop("realtimeFrequency")
            ob.pop("epoch")
            try:
                self.raw.insert_one(ob)
                self.logger.info(str(ob))
            except Exception as e:
                self.logger(str(e))
                self.logger.info("duplicate current post")

    def get_day(self):
        """
        Get observations for the full day
        """
        url = (
            "https://api.weather.com/v2/pws/observations/all/1day?stationId="
            + str(self.sid)
            + "&format=json&units=e&apiKey="
            + str(self.api)
        )
        hdr = {
            "User-Agent": "Mozilla/5.0 (Macintosh; \
                Intel Mac OS X 10_10_1) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/39.0.2171.95 \
                Safari/537.36"
        }
        req = urllib.request.Request(url, headers=hdr)
        msg = urllib.request.urlopen(req).read()
        msg = json.loads(msg)
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
                self.logger.info(str(ob))
            except Exception as e:
                self.logger(str(e))
                self.logger.info("duplicate day post")

    def run(self):
        """
        Main method to run
        """
        client = MongoClient(self.dbc)
        db = client.wx
        self.raw = db.raw

        last_hour = datetime.now(timezone.utc).hour - 1
        last_minute = datetime.now(timezone.utc).minute - 1
        while True:
            if datetime.now(timezone.utc).minute != last_minute:
                try:
                    self.get_current()
                    last_minute = datetime.now(timezone.utc).minute
                    self.logger.info(
                        "got current " + str(datetime.now(timezone.utc))
                    )
                except Exception:
                    self.logger.info(
                        "failed current " + str(datetime.now(timezone.utc))
                    )
                    pass
            else:
                self.logger.info(
                    "skipping current " + str(datetime.now(timezone.utc))
                )
            if datetime.now(timezone.utc).hour != last_hour:
                try:
                    self.get_day()
                    last_hour = datetime.now(timezone.utc).hour
                    self.logger.info(
                        "got day " + str(datetime.now(timezone.utc))
                    )
                except Exception:
                    self.logger.info(
                        "failed day " + str(datetime.now(timezone.utc))
                    )
                    pass
            else:
                self.logger.info(
                    "skipping day " + str(datetime.now(timezone.utc))
                )
            time.sleep(30)


if __name__ == "__main__":
    wunderground = Wunderground(
        dbc=os.environ["MONGODB_CLIENT"],
        sid=os.environ["SID"],
        api=os.environ["API"],
    )
    wunderground.run()
