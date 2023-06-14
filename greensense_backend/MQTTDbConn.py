import paho.mqtt.client as paho
import greensense_backend.DbConnection as Db
import threading
import time


# dev/{mac}/{type}/{id} - wiadomosc od urządzeń
# msg i w body polecenie

# subskrybowanie wildcarda dev/+/+/+

# # - multi level wildcard
# + - single level wildcard

# no i zapis do bazki mac - adres mac urządzenia, type - jakis predefiniowany typ, id
# create table mqtt_logs (log_id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER NOT NULL, mac_addr TEXT NOT NULL, type TEXT, sensor_id INTEGER, reading TEXT NOT NULL)

class MQTTDbConn(threading.Thread):
    def __init__(self, dbname, ipAddr, port, topics, username, password):
        threading.Thread.__init__(self)
        self.dbName = dbname
        self.ipAddr = ipAddr
        self.port = port
        self.topics = topics
        self.dbConn = Db.DbConnection(self.dbName)
        self.client = paho.Client()
        self.username = username
        self.password = password

    def run(self):
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        self.client.username_pw_set(self.username, self.password)
        self.client.connect(self.ipAddr, self.port, 60)
        print(f"Connection with {self.ipAddr}:{self.port} successfull, listening on topic dev/#")
        for topic in self.topics:
            self.client.subscribe(topic, 0)
        while self.client.loop() == 0:
            pass

    def on_message(self, mosq, obj, msg):
        # topic standard is dev/{mac}/{type}/{id} so after split should be [0]=dev [1]=mac [2]=type [3]=id
        topicTokens = msg.topic.split('/')
        if len(topicTokens) != 4 or topicTokens[2] == "cmd" or topicTokens[3] == 'num':
            return
        # payload is a string
        self.saveMqttLog(topicTokens[1], topicTokens[2], topicTokens[3], msg.payload)

    def on_publish(self, mosq, obj, mid):
        pass

    def getUniqueMacAddresses(self):
        return self.dbConn.executeSQL("SELECT DISTINCT mac_addr, min(timestamp), max(timestamp) FROM mqtt_logs GROUP BY mac_addr")

    def getTimestampsForMacAddress(self, mac_address):
        return self.dbConn.executeSQL(f"SELECT DISTINCT mac_addr, min(timestamp), max(timestamp) FROM mqtt_logs WHERE mac_addr='{mac_address}' GROUP BY mac_addr")

    def getTimestampsForMacAddressAndType(self, mac_address, provided_type):
        return self.dbConn.executeSQL(f"SELECT DISTINCT mac_addr, min(timestamp), max(timestamp) FROM mqtt_logs WHERE mac_addr='{mac_address}' and type='{provided_type}' GROUP BY mac_addr ")

    def getAllDataByType(self, type):
        return self.dbConn.executeSQL(f"SELECT * FROM mqtt_logs WHERE type = '{type}'")

    def getAllDataByMac(self, mac_addr):
        return self.dbConn.executeSQL(f"SELECT * FROM mqtt_logs WHERE mac_addr='{mac_addr}'")

    def getDataByMacAndType(self, mac_addr, type):
        return self.dbConn.executeSQL(f"SELECT * FROM mqtt_logs WHERE mac_addr='{mac_addr}' and type='{type}'")

    def getDataByMacTypeAndSensorID(self, mac_addr, type, sensorID):
        return self.dbConn.executeSQL(f"SELECT * FROM mqtt_logs WHERE mac_addr='{mac_addr}' and type='{type}' and id='{sensorID}'")

    def getDataByMacAddress(self, mac_addr):
        return self.dbConn.executeSQL(f"SELECT * FROM mqtt_logs WHERE mac_addr='{mac_addr}'")

    def getSubSensorsIdsByMacAddressAndType(self, mac_addr, type):
        return self.dbConn.executeSQL(f"SELECT DISTINCT sensor_id FROM mqtt_logs WHERE mac_addr='{mac_addr}' and type='{type}'")

    def getAllData(self):
        return self.dbConn.executeSQL("SELECT * FROM mqtt_logs")

    def deleteAllData(self):
        return self.dbConn.executeSQL("DELETE FROM mqtt_logs WHERE log_id != -1")

    def printAllLogs(self):
        for row in self.dbConn.executeSQL("SELECT * FROM mqtt_logs"):
            print(row)

    def saveMqttLog(self, mac, type, id, reading):
        params = (int(time.time()), mac, type, id, reading)
        self.dbConn.executeSQL(f"INSERT INTO mqtt_logs VALUES (null, ?, ?, ?, ?, ?)", params)
        self.dbConn.commit()
