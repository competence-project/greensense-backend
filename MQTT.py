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
    #TODO: list with valid types used in our system
    types = ['temp', 'illum']

    # loopback
    # mqttClient = MQTTDbConn("testdb", "127.0.0.1", 1883, "dev/#")
    # at my own place
    # mqttClient = MQTTDbConn("testdb", "192.168.100.10", 1883, "dev/#")
    # at school
    mqttClient = MQTTDbConn("testdb", "10.1.4.168", 1883, "dev/#")
    mqttClient.start()

    app = FastAPI()
    #sample, test endpoint
    @app.get("/")
    async def root():
        return {"message": "Hello World."}

    #endpoint returning data from all sensors as json
    @app.get("/data/all")
    async def root():
        #list that is supposed to contain tuples with (mac, minimum timestamp, max timestamp)
        macsAndTimestamps = []

        #prints mac_addr, min and max timestamps as well as appends them to list
        for row in mqttClient.getUniqueMacAddresses():
            print(row)
            macsAndTimestamps.append(row)

        #loops that print full data rows for each sensor
        for mac in macsAndTimestamps:
            #mac[0] is mac address from tuple mentioned in fist comment in this method
            for mac_row in mqttClient.getAllDataForSensor(mac[0]):
                print(mac_row)
            #delimiter between each distinct mac address
            print("===")

        #response list
        ret_data = []

        #appending data to return list in format specified in our system
        for mac in macsAndTimestamps:
            #first append min, max datetime for each sensor, assign timezone offset and specify sub-sensor Id
            tmp_dict = {'datetime': {'from': mac[1], 'to': mac[2], 'timezone_offset': 7200}, 'sensor': {"mac_address": mac[0]}}
            data = []
            for measurementType in types:
                #gathering distinct ids of sub-sensors cuz only one cursor can be opened at once
                ids = []
                for idTuple in mqttClient.getSubSensorsIdsByMacAddressAndType(mac[0], measurementType):
                    ids.append(idTuple[0])
                #appending data from each sub-sensor with given type to return json
                for id in ids:
                    dataForType = {'name': measurementType, 'id': id}
                    measurement_list = []
                    for mac_row in mqttClient.getAllDataForSensor(mac[0]):
                        if mac_row[3] == measurementType and mac_row[4] == id:
                            measurement_list.append({'datetime': mac_row[1], 'result': mac_row[5]})
                    dataForType['measurement_list'] = measurement_list
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
