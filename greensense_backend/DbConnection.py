import sqlite3


class DbConnection:
    def __init__(self, database):
        self.conn = con = sqlite3.connect(database, check_same_thread=False)
        self.cur = con.cursor()
        self.cur.execute("create table if not exists mqtt_logs (log_id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER NOT NULL, mac_addr TEXT NOT NULL, type TEXT, sensor_id INTEGER, reading TEXT NOT NULL)")

    def executeSQL(self, expression, *args):
        if len(args) > 0:
            return self.cur.execute(expression, *args)
        else:
            return self.cur.execute(expression)

    def closeConn(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()
