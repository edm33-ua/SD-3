from flask import Flask, request, jsonify
import threading
import tkinter as tk
import sqlite3
import hashlib

################ PATHS/CONSTANTS  ################

Certificate = 'RegistryCertificate/cert.pem'
Key = 'RegistryCertificate/key.pem'
DATABASE = 'data/database.db'
dbLock = threading.Lock()

################  TABLE CREATIONS  ###############
# DESCRIPTION: Este comprueba que exista la tabla Registry en la DB. Si no existe, la crea
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def checkRegistryTableDB():
    global dbLock
    try:
        with dbLock:
            # We first connect to the database
            conn = sqlite3.connect(DATABASE)
            # After that, we create the cursor to handle the database
            c = conn.cursor()
            # We check wheter the table registry is created or not
            c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='Registry'")
            exists = c.fetchone()
            # In case it exists, we print that the table already exists,
            if exists:
                print(f'[DATABASE HANDLER] Table Registry found')
            # but in case it doesn't, we create the table
            else:
                print(f'[DATABASE HANDLER] Table Registry not found. Proceeding to table creation...')
                c.execute("CREATE TABLE Registry (id text PRIMARY KEY, password text NOT NULL)")
            # We commit the changes done to the database
            conn.commit()
            # Then we close the database
            conn.close()
    except Exception as e:
        print(f'[DATABASE MANAGER] THERE HAS BEEN AN ERROR WHILE GETTING THE DATABASE TABLES. {e}')
        conn.close()

################  API DEFINITION  ################

app = Flask(__name__)
# DESCRIPTION: Este método dará de alta al taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
@app.route('/addTaxi', methods=['POST'])
def addTaxi():
    try:
        global dbLock
        # We first get the data payload from the request
        data = request.json
        # Then we extract the information from it
        id = data.get('id')
        password = hashlib.md5(('*/' + data.get('password') + '/*').encode()).hexdigest()
        # Now we connect to the database
        with dbLock:
            conn = sqlite3.connect(DATABASE)
            # We get the cursor to handle to DB connection
            c = conn.cursor()
            # And make sure to fetch all the entries of the DB
            c.execute(f"SELECT * FROM Registry")
            entries = [item for item in c.fetchall()]
            flag = False
            # Then we compare the id sent with every existing id, in case the taxi is already registered
            for entry in entries:
                # If it is, we set the flag to True
                if entry[0] == id:
                    flag = True
            if not flag:
                c.execute(f"INSERT INTO Registry VALUES ('{id}', '{password}')")
                response = jsonify({'response': 'OK'})
            else:
                response = jsonify({'response': 'KO'})
            # Then we commit the changes and close the connection
            conn.commit()
            conn.close()
        return response
    except Exception as e:
        print(f"[TAXI DELETION] THERE HAS BEEN AN ERROR WHEN TRYING TO ADD A TAXI. {e}")
        response = jsonify({'response': 'ERR'})
        return response

# DESCRIPTION: Este método dará de baja al taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
@app.route('/deleteTaxi', methods=['POST'])
def deleteTaxi():
    try:
        global dbLock
        # We first get the data payload from the request
        data = request.json
        # Then we extract the information from it
        id = data.get('id')
        password = hashlib.md5(('*/' + data.get('password') + '/*').encode()).hexdigest()
        # Now we connect to the database
        with dbLock:
            conn = sqlite3.connect(DATABASE)
            # We get the cursor to handle to DB connection
            c = conn.cursor()
            # And make sure to fetch all the entries of the DB
            c.execute(f"SELECT * FROM Registry")
            entries = [item for item in c.fetchall()]
            flag = False
            # Then we compare the id sent with every existing id, in case the taxi is already registered
            for entry in entries:
                # If it is, we set the flag to True
                if entry[0] == id and entry[1] == password:
                    flag = True
            if flag:
                c.execute(f"DELETE FROM Registry WHERE id = '{id}'")
                response = jsonify({'response': 'OK'})
            else:
                response = jsonify({'response': 'KO'})
            # Then we commit the changes and close the connection
            conn.commit()
            conn.close()
        return response
    except Exception as e:
        print(f"[TAXI DELETION] THERE HAS BEEN AN ERROR WHEN TRYING TO DELETE A TAXI. {e}")
        response = jsonify({'response': 'ERR'})
        return response
    
#########  MAIN STARTING POINT  #########
if __name__ == '__main__':
    # We check for the DB
    checkRegistryTableDB()
    # We begin running the application
    try:
        app.run(port=5000, ssl_context=(Certificate, Key), host='0.0.0.0')
    except Exception as e:
        print(f'[APPLICATION HANDLER] THERE HAS BEEN AN ERROR WHILE HANDLING THE APPLICATION. {e}')