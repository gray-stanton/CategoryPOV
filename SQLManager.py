import yaml
import sqlite3 as sqlite
import pymysql
def get_confs(conf_yaml_file = "sqlserver.yaml"):
    try:
        with open(conf_yaml_file, "rb") as yaml_file:
            confs = yaml.load(yaml_file)
    except FileNotFoundError:
        print("ERROR: Can't find SQL server config file {}".format(
            conf_yaml_file))
        raise
    else:
        return confs

def query_server(sql_query):
    try:
        confs = get_confs()
        db = pymysql.connect(**confs)
        with db.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    except IOError:
        print("ERROR: Can't get results from SQL server.")
    finally:
        db.close()
        return result
