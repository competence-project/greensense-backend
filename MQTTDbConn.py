import paho.mqtt.client as paho
import DbConnection as Db
import threading
import util
import time


# dev/{mac}/{type}/{id} - wiadomosc od urządzeń
# msg i w body polecenie

# suboskrybowanie wildcarda dev/#

# # - multi level wildcard
# + - single level wildcard

# no i zapis do bazki mac - adres mac urządzenia, type - jakis predefiniowany typ, id
# create table mqtt_logs (log_id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER NOT NULL, mac_addr TEXT NOT NULL, type TEXT, sensor_id INTEGER, reading REAL NOT NULL)

class MQTTDbConn(threading.Thread):
    def __init__(self, dbname, ipAddr, port, topics):
        threading.Thread.__init__(self)
        self.dbName = dbname
        self.ipAddr = ipAddr
        self.port = port
        self.topics = topics
        self.dbConn = Db.DbConnection(self.dbName)
        self.client = paho.Client()

    def run(self):
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        self.client.connect(self.ipAddr, self.port, 60)
        for topic in self.topics:
            self.client.subscribe(topic, 0)
        while self.client.loop() == 0:
            pass

    def on_message(self, mosq, obj, msg):
        # print("%-20s %d %s" % (msg.topic, msg.qos, msg.payload))
        # mosq.publish('pong', 'ack', 0)
        # print("===== ALL DATA STORED IN THE DATABASE: =====")
        # self.printLogs()
        # print('')

        topicTokens = msg.topic.split('/')
        self.saveMqttLog(topicTokens[1], topicTokens[2], topicTokens[3], float(util.convertBytesToString(msg.payload)))

    def on_publish(self, mosq, obj, mid):
        pass

    def getUniqueMacAddresses(self):
        return self.dbConn.executeSQL("SELECT DISTINCT mac_addr, min(timestamp), max(timestamp) FROM mqtt_logs GROUP BY mac_addr")

    def getAllDataForSensor(self, mac_addr):
        return self.dbConn.executeSQL(f"SELECT * FROM mqtt_logs WHERE mac_addr='{mac_addr}'")

    def getAllDataTest(self):
        return self.dbConn.executeSQL("SELECT * FROM mqtt_logs")

    def getAllData(self):
        return self.dbConn.executeSQL("SELECT * FROM mqtt_logs")

    def getTempData(self):
        return self.dbConn.executeSQL("SELECT * FROM mqtt_logs WHERE type = 'temp'")

    def deleteAllData(self):
        return self.dbConn.executeSQL("DELETE FROM mqtt_logs WHERE log_id != -1")

    def printAllLogs(self):
        for row in self.dbConn.executeSQL("SELECT * FROM mqtt_logs"):
            print(row)

    def saveMqttLog(self, mac, type, id, reading):
        params = (int(time.time()), mac, type, id, reading)
        self.dbConn.executeSQL(f"INSERT INTO mqtt_logs VALUES (null, ?, ?, ?, ?, ?)", params)
        self.dbConn.commit()
