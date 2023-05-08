from typing import List

from pydantic import BaseModel
from MQTTDbConn import MQTTDbConn
from fastapi import FastAPI
import uvicorn


class MQTTData(BaseModel):
    log_id: int
    timestamp: int
    mac_addr: str
    type: str
    sensor_id: int
    reading: float


if __name__ == "__main__":
    types = ['temp', 'lum']

    mqttClient = MQTTDbConn("testdb", "127.0.0.1", 1883, "dev/#")
    mqttClient.start()

    app = FastAPI()

    @app.get("/")
    async def root():
        return {"message": "Hello World."}


    @app.get("/data/all")
    async def root():
        macs = []
        # returns mac_addr and min, max timestamps
        for row in mqttClient.getUniqueMacAddresses():
            print(row)
            macs.append(row)

        for mac in macs:
            for mac_row in mqttClient.getAllDataForSensor(mac[0]):
                print(mac_row)
            print("===")

        # extract min and max data (begin, end) - ok
        # TODO: build json according to szymonaszek's mock (to be talked about)
        ret_data = []

        for mac in macs:
            tmp_dict = {'datetime': {'from': mac[1], 'to': mac[2], 'timezone_offset': 7200}}
            tmp_dict['sensor'] = {"mac_address": mac[0], "localization": 'tbd'}
            data = []
            for type in types:
                dataForType = {'name': type}
                measurement_list = []
                for mac_row in mqttClient.getAllDataForSensor(mac[0]):
                    if mac_row[3] == type:
                        measurement_list.append({'datetime': mac_row[1], 'result': mac_row[5]})
                dataForType['measurements'] = measurement_list
                data.append(dataForType)
            tmp_dict['data'] = data
            ret_data.append(tmp_dict)

        return ret_data


    @app.get("/data/test", response_model=List[MQTTData])
    async def root():
        ret = mqttClient.getAllDataTest()
        data = []
        for row in ret:
            data.append(MQTTData(log_id=row[0], timestamp=row[1], mac_addr=row[2], type=row[3], sensor_id=row[4], reading=row[5]))

        return data

    @app.get("/data/temp", response_model=List[MQTTData])
    async def root():
        ret = mqttClient.getTempData()
        data = []
        for row in ret:
            data.append(MQTTData(log_id=row[0], timestamp=row[1], mac_addr=row[2], type=row[3], sensor_id=row[4],
                                 reading=row[5]))

        return data

    uvicorn.run(app, host="127.0.0.1", port=8000)
