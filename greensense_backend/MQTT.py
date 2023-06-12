from greensense_backend.MQTTDbConn import MQTTDbConn
from fastapi import FastAPI
import uvicorn
import sys

def start():
    types = ['temp', 'illum', 'pssr', 'hum']

    # mqttClient = MQTTDbConn("testdb", "127.0.0.1", 1883, "dev/#")
    # mqttClient = MQTTDbConn("testdb", "192.168.100.10", 1883, "dev/#")
    # ip is given with args, if not use hardcoded value
    ip = sys.argv[1] if len(sys.argv) >= 2 else "127.0.0.1"
    username = sys.argv[2] if len(sys.argv) >= 3 else "admin"
    password = sys.argv[3] if len(sys.argv) >= 4 else "admin"
    mqttClient = MQTTDbConn("testdb", ip, 1883, "dev/#", username, password)

    mqttClient.start()
    app = FastAPI()


    # sample, test endpoint
    @app.get("/")
    async def root():
        return {"message": "Hello World."}


    # endpoint returning data from all sensors as json
    @app.get("/data/all")
    async def getAllData():
        # list that is supposed to contain tuples with (mac, minimum timestamp, max timestamp)
        macsAndTimestamps = []

        # prints mac_addr, min and max timestamps as well as appends them to list
        for row in mqttClient.getUniqueMacAddresses():
            print(row)
            macsAndTimestamps.append(row)

        # loops that print full data rows for each sensor
        for mac in macsAndTimestamps:
            # mac[0] is mac address from tuple mentioned in fist comment in this method
            for mac_row in mqttClient.getAllDataByMac(mac[0]):
                print(mac_row)
            # delimiter between each distinct mac address
            print("===")

        # response list
        ret_data = []

        # appending data to return list in format specified in our system
        for mac in macsAndTimestamps:
            # first append min, max datetime for each sensor, assign timezone offset and specify sub-sensor Id
            tmp_dict = {'datetime': {'from': mac[1], 'to': mac[2], 'timezone_offset': 7200},
                        'sensor': {"mac_address": mac[0]}}
            data = []
            for measurementType in types:
                # gathering distinct ids of sub-sensors cuz only one cursor can be opened at once
                ids = []
                for idTuple in mqttClient.getSubSensorsIdsByMacAddressAndType(mac[0], measurementType):
                    ids.append(idTuple[0])
                # appending data from each sub-sensor with given type to return json
                for id in ids:
                    dataForType = {'name': measurementType, 'id': id}
                    measurement_list = []
                    for mac_row in mqttClient.getAllDataByMac(mac[0]):
                        if mac_row[3] == measurementType and mac_row[4] == id:
                            measurement_list.append({'datetime': mac_row[1], 'result': mac_row[5]})
                    dataForType['measurement_list'] = measurement_list
                    data.append(dataForType)
            tmp_dict['data'] = data
            ret_data.append(tmp_dict)

        return ret_data


    # endpoint returning data for given sensor as json
    @app.get("/data/{mac_addr}")
    async def getDataByMac(mac_addr):
        # list that is supposed to contain tuples with (mac, minimum timestamp, max timestamp)
        macAndTimestamps = []

        # prints mac_addr, min and max timestamps as well as appends them to list
        for row in mqttClient.getTimestampsForMacAddress(mac_addr):
            macAndTimestamps = row

        if len(macAndTimestamps) < 1:
            return {}

        # response list
        ret_data = []

        # first append min, max datetime for each sensor, assign timezone offset and specify sub-sensor Id
        tmp_dict = {'datetime': {'from': macAndTimestamps[1], 'to': macAndTimestamps[2], 'timezone_offset': 7200},
                    'sensor': {"mac_address": macAndTimestamps[0]}}
        data = []
        for measurementType in types:
            # gathering distinct ids of sub-sensors cuz only one cursor can be opened at once
            ids = []
            for idTuple in mqttClient.getSubSensorsIdsByMacAddressAndType(macAndTimestamps[0], measurementType):
                ids.append(idTuple[0])
            # appending data from each sub-sensor with given type to return json
            for id in ids:
                dataForType = {'name': measurementType, 'id': id}
                measurement_list = []
                for mac_row in mqttClient.getDataByMacAddress(macAndTimestamps[0]):
                    if mac_row[3] == measurementType and mac_row[4] == id:
                        measurement_list.append({'datetime': mac_row[1], 'result': mac_row[5]})
                dataForType['measurement_list'] = measurement_list
                data.append(dataForType)
        tmp_dict['data'] = data
        ret_data.append(tmp_dict)

        return ret_data


    # endpoint returning data from for given sensor with given type as json
    @app.get("/data/{mac_addr}/{type}")
    async def getDataByMacAndType(mac_addr, sensorType):
        # list that is supposed to contain tuples with (mac, minimum timestamp, max timestamp)
        macAndTimestamps = []

        # prints mac_addr, min and max timestamps as well as appends them to list
        for row in mqttClient.getTimestampsForMacAddressAndType(mac_addr, sensorType):
            macAndTimestamps = row

        if len(macAndTimestamps) < 1:
            return {}

        # response list
        ret_data = []

        # first append min, max datetime for each sensor, assign timezone offset and specify sub-sensor Id
        tmp_dict = {'datetime': {'from': macAndTimestamps[1], 'to': macAndTimestamps[2], 'timezone_offset': 7200},
                    'sensor': {"mac_address": macAndTimestamps[0]}}
        data = []
        # gathering distinct ids of sub-sensors cuz only one cursor can be opened at once
        ids = []
        for idTuple in mqttClient.getSubSensorsIdsByMacAddressAndType(macAndTimestamps[0], sensorType):
            ids.append(idTuple[0])
        # appending data from each sub-sensor with given type to return json
        for id in ids:
            dataForType = {'name': sensorType, 'id': id}
            measurement_list = []
            for mac_row in mqttClient.getDataByMacAndType(macAndTimestamps[0], sensorType):
                if mac_row[3] == sensorType and mac_row[4] == id:
                    measurement_list.append({'datetime': mac_row[1], 'result': mac_row[5]})
            dataForType['measurement_list'] = measurement_list
            data.append(dataForType)
        tmp_dict['data'] = data
        ret_data.append(tmp_dict)

        return ret_data


    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    start()