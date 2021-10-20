import sys
import mysql.connector
import os
import re

def main():
    try:
        script_path = sys.argv[1]
        db_user = sys.argv[2]
        db_host = sys.argv[3]
        db_name = sys.argv[4]
        db_password = sys.argv[5]
    except:
        print("Please provide Database details and script path.")
        sys.exit(1)

    table_name = 'versionTable'  
    db_connection = get_db_connection(db_user, db_host, db_name, db_password)
    latest_db_version = get_latest_version_from_db(db_connection, table_name)
    
    scripts = get_scripts(script_path)

    for script in scripts:
        script_number = int(script[0:2])
        if script_number > latest_db_version:
            outcome = run_script(db_connection, script_path, script)
            if outcome == "success":
                update_table(db_connection, table_name, script_number)
            
     
   
def run_script(db_connection, script_path, script):
    print("Running Script")
    try:
        db_cursor = db_connection.cursor()
        sql_script = script_path + '/' + script
        with open(sql_script,'r') as statements:
            sql = statements.read()
        db_cursor.execute(sql)
        db_connection.commit()
        print(f"Successfully ran {script}")
        return "success"
    except mysql.connector.Error as error:
        print(f"ERROR! Unable to run {script} \n {error}")
        return "failed"



def update_table(db_connection, table_name, script_version):
    print("Updating table")
    try:
        db_cursor = db_connection.cursor()
        db_cursor.execute(f"INSERT INTO {table_name} \
                                VALUES ({script_version})")
        db_connection.commit()
        print(f"{table_name} has been successfully updated with {script_version}")
    except mysql.connector.Error as error:
        print(f"ERROR! {error}")
        
  
def get_latest_version_from_db(db_connection, table_name):
    print("Getting latest version from DB")
    try:
        db_cursor = db_connection.cursor()
        db_cursor.execute(f"SELECT * FROM {table_name}")
        result = db_cursor.fetchall()
        latest_version = max(result)
        print(f"LATEST DB VERSION: {latest_version[0]}")
        return latest_version[0]
    except mysql.connector.Error as error:
        print(f"ERROR! {error}")
        sys.exit(1)


def get_scripts(script_path):
    print("Getting scripts")
    scripts_list = []
    
    try:
        for file in os.listdir(script_path):
            scripts = re.findall(r'\d[0-9].*\.sql', file)
            for script in scripts:
                scripts_list.append(script)
        return scripts_list
    except:
        print("Error! I am not able to retrieve any script.")
        sys.exit(1)


def get_db_connection(db_user, db_host, db_name, db_password):
    print("Getting Db connection")
    try:
        connection = mysql.connector.connect(user=db_user,
                                        database=db_name,
                                        password=db_password,
                                        host=db_host)
        return connection
    except mysql.connector.Error as error:
        print(f"ERROR! {error}")
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())