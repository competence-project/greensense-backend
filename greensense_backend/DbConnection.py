import sqlite3
import os
import platform


class DbConnection:
    def __init__(self, database):
        directory = ".greensense"
        home_path = os.path.expanduser("~")

        new_dir_path = home_path
        if platform.system() == "Linux":
            new_dir_path = os.path.join(home_path, ".local", directory)
        elif platform.system() == "Windows":
            new_dir_path = os.path.join(home_path, "AppData", "Roaming", directory)
        else:
            new_dir_path = os.path.join(home_path, directory)

        if not os.path.exists(new_dir_path):
            os.mkdir(new_dir_path, mode=0o777)

        self.conn = sqlite3.connect(os.path.join(new_dir_path, database), check_same_thread=False)
        self.cur = self.conn.cursor()
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
