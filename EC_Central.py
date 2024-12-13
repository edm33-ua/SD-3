import sys
import sqlite3
# TABLE STRUCTURE FOR DDBB
# Taxis: id (text) PRIMARY KEY, destination (text), state (text), service (text), client (text), active (text), mounted (text), pos (text)
# Clients: id (text) PRIMARY KEY, destination (text), state (text), taxi (text), pos (text)
from kafka import KafkaConsumer, KafkaProducer
import socket
import threading
import json
import time
import tkinter as tk
from tkinter import ttk
import encodings.idna
import ssl
from secrets import token_hex
from cryptography.fernet import Fernet
import hashlib
import requests

# SERVER = "172.21.242.82"
MAP_ROWS = 20
MAP_COLUMNS = 20
DATABASE = 'data/database.db'
HEADER = 1024
FORMAT = 'utf-8'
TABLENAMES = ['Taxis', 'Clients', 'Registry']
LOCATION_FILE = 'EC_locations.json'
SECONDS = 10
CERTIFICATE_FOLDER = 'Certificates'

global dbLock, internalMemory, memLock, locationDictionary, locLock, clientMapLocation, clientLock
global connDictionary, connDicLock
global taxiTableGlobal, selectedTaxi, selectedPos
global clientConnections, clientConnectionsLock
global weatherState, weatherState2
global taxiSessions

############ LOCAL CLASSES ############

# Atributes:
# ID
# State
# Active
# OnService
# Client
# Mounted
# Pos
class Taxi:

    # Constructor
    def __init__(self, id, state, active, onService, client, mounted, pos):
        self.id = id
        self.state = state
        self.active = active
        self.onService = onService
        self.client = client
        self.mounted = mounted
        self.pos = pos
    
    # Method to represent the Taxi when in list
    def __repr__(self) -> str:
        return f"<TAXI {self.id}: State:{self.state}, Active:{self.active}, Service:{self.onService}, Client:{self.client}, Mounted:{self.mounted} , Position:{self.pos}>"
    
    # Method to represent the Taxi when printing
    def __str__(self) -> str:
        return f"<TAXI {self.id}: State:{self.state}, Active:{self.active}, Service:{self.onService}, Client:{self.client}, Mounted:{self.mounted} , Position:{self.pos}>"

# Atributes:
# ID
# Destination
# State
class Client:

    # Constructor
    def __init__(self, id, destination, state):
        self.id = id
        self.destination = destination
        self.state = state
    
    # Method to represent the Client when in list
    def __repr__(self) -> str:
        return f"<CLIENT {self.id}: Destination:{self.destination}, State:{self.state}>"
    
    # Method to represent the Client when printing
    def __str__(self) -> str:
        return f"<CLIENT {self.id}: Destination:{self.destination}, State:{self.state}>"

############ CTC WEATHER CONTROL ############

# DESCRIPTION: Este método lo que hace es llamar a la API de EC_CTC y comprobar su respuesta, enviando un BADWEATHER o GOODWEATHER,
# solo al haber un cambio, por el tópico de kafka para las órdenes de los taxis
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def weatherManager():
    global weatherState, weatherState2, SERVER
    producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
    # We define the url to call
    url = "https://" + SERVER + ":5000/getWeather"
    while True:
        # Calls the CTC API
        response = requests.get(url).json()
        # Disects the response
        if response["response"] == 'OK':
            weatherState2 = 'GOOD'
        elif response["response"] == 'KO':
            weatherState2 = 'BAD'
        if weatherState != weatherState2:
            if weatherState2 == 'GOOD':
                # Enviar el GOODWEATHER
                producer.send()
            else:
                # Enviar el BADWEATHER
                producer.send()
        # Equals the before state to the current state
        weatherState = weatherState2
        time.sleep(10)

############ JSON FILE MANAGEMENT ############

# DESCRIPTION: Este método lo que hace crear el diccionario interno para las localizaciones
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def readJsonConfigurationFile():
    global locationDictionary, locLock

    try:
        print(f"[JSON FILE MANAGER] Reading .json location file")
        # Open and read the JSON file
        f = open(LOCATION_FILE)
        data = json.load(f)
        # Print the data
        f.close()
        # Semaphore for locationDictionary
        locLock.acquire()
        # For each location in the file, we create a new key in the locationDictionary and associate the value in the 000 format
        for location in data['locations']:
            key = location['Id']
            stringValue = location['POS'].split(',')
            value = int(stringValue[0])*20 + int(stringValue[1])
            locationDictionary.update({key: value})
        locLock.release()
        print(f'[JSON FILE MANAGER] Succesful creation of the internal dictionary for the locations')

    except Exception as e:
        print(f"[JSON FILE MANAGER] THERE HAS BEEN AN ERROR WHILE READING JSON FILE: {e}")
        f.close()

############ DATABASE/MEMORY METHODS ############

# DESCRIPTION: Este método comprueba si un taxi está en la tabla Registry, devolviendo un valor numérico en base a su estado
# STARTING_VALUES: id del taxi, contraseña del taxi
# RETURNS: -1, si el taxi no está en la tabla, 0 si el taxi está pero la contraseña es incorrecta, 1 si el taxi está y la contraseña
# es correcta
# NEEDS: NONE
def findInRegistry(id, password):
    try:
        global dbLock
        # We convert the password to its equivalent hash
        password = hashlib.md5(('*/' + password+ '/*').encode()).hexdigest()
        # Now we connect to the database
        with dbLock:
            conn = sqlite3.connect(DATABASE)
            # We get the cursor to handle to DB connection
            c = conn.cursor()
            # And make sure to fetch all the entries of the DB
            c.execute(f"SELECT * FROM Registry")
            entries = [item for item in c.fetchall()]
            # We declare a flag to check if the taxi exists in the DB
            flag = False
            for entry in entries:
                if entry[0] == id:
                    flag = True
                    if entry[1] == password:
                        return 1
            conn.close()
        if flag:
            return 0
        else:
            return -1
    except Exception as e:
        print(f"[DATABASE HANDLER] THERE HAS BEEN AN ERROR WHEN TRYING TO FIND A TAXI IN THE REGISTRY TABLE. {e}")

# DESCRIPTION: Este método inicia los valores de la memoria interna con los de la base de datos. Su propósito es ser capaz
# de retomar las acciones que estaba realizando cuando la central se desconecte
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def initializeInnerMemory():
    global internalMemory, memLock, dbLock, connDictionary, connDicLock, clientMapLocation, clientLock

    try:
        # We first connect to the database
        dbLock.acquire()
        conn = sqlite3.connect(DATABASE)
        # After that, we create the cursor to handle the database
        c = conn.cursor()
        # We first read all the taxis from the db, and insert them in the internalMemory
        c.execute(f"SELECT * FROM Taxis")
        # We format them nicely in a list
        taxiList = [item for item in c.fetchall()]
        # Then we read all the clients from the db, and insert them in the internalMemory
        c.execute(f"SELECT * FROM Clients")
        # We format them nicely in a list
        clientList = [item for item in c.fetchall()]
        memLock.acquire()
        connDicLock.acquire()
        # Then, for each item in the list, we introduce it in the internal memory
        for item in taxiList:
            taxi = Taxi(item[0], item[2], item[5], item[3], item[4], item[6], item[7])
            internalMemory.update({taxi.id: taxi})
            connDictionary.update({taxi.id: 0})
        connDicLock.release()

        clientLock.acquire()
        for item in clientList:
            client = Client(item[0], item[1], item[2])   
            internalMemory.update({client.id: client})
            if item[4] != '-':
                clientMapLocation.update({client.id: item[4]})
        # We release the semaphores and close the connection
        clientLock.release()
        memLock.release()
        dbLock.release()
        conn.close()
    except Exception as e:
        print(f"[DATABASE MANAGER] THERE HAS BEEN A PROBLEM WHILE LOADING THE DDBB INFORMATION IN THE INTERNAL MEMORY. {e}")

# DESCRIPTION: Este método acorta el código en el método refresh. Realiza el proceso de borrado
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def refreshDelete():
    global c, taxis, clients
    try:

        # Now, what we do is, for each id from the taxis and clients that we don't find in the internal memory, we delete them
        for taxi in taxis:
            if taxi not in internalMemory:
                c.execute(f"DELETE FROM Taxis WHERE id = '{taxi}'")
                taxis.remove(taxi)

        for client in clients:
            if client not in internalMemory:
                c.execute(f"DELETE FROM Clients WHERE id = '{client}'")
                clients.remove(client)
    except Exception as e:
        print(f'[DATABASE MANAGER] THERE HAS BEEN AN ERROR WHILE ADDING TO THE DATABASE TABLES. {e}')

# DESCRIPTION: Este método acorta el código en el método refresh. Realiza el proceso de actualizado
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def refreshUpdate():
    global c, clientTaxiDictionary, taxis, clients, clientMapLocation, clientLock
    try:
        # Now, for each taxi and client that remains, we update their values
        for taxi in taxis:
            if taxi in internalMemory:
                thisTaxi = internalMemory[taxi]
                if thisTaxi.client != '-':
                    clientTaxiDictionary.update({thisTaxi.client: taxi})
                    if thisTaxi.mounted == 'Y':
                        destination = internalMemory[thisTaxi.client].destination
                    else:
                        destination = thisTaxi.client    
                else:
                    destination = '-'
                c.execute(f"UPDATE Taxis SET destination = '{destination}', state = '{thisTaxi.state}'," + 
                    f"service = '{thisTaxi.onService}', client = '{thisTaxi.client}', active = '{thisTaxi.active}', " + 
                    f"mounted = '{thisTaxi.mounted}', pos = '{thisTaxi.pos}' WHERE id = '{taxi}'")

        clientLock.acquire()
        for client in clients:
            if client in internalMemory:
                thisClient = internalMemory[client]
                position = '-'
                if client in clientMapLocation:
                    position = str(clientMapLocation[client])
                c.execute(f"UPDATE Clients SET destination = '{thisClient.destination}', state = '{thisClient.state}'," +
                    f"taxi = '{clientTaxiDictionary[client]}', pos = '{position}' WHERE id = '{client}'")
        clientLock.release()
    except Exception as e:
        print(f'[DATABASE MANAGER] THERE HAS BEEN AN ERROR WHILE ADDING TO THE DATABASE TABLES. {e}')
# DESCRIPTION: Este método acorta el código en el método refresh. Realiza el proceso de adición
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def refreshAdd():
    global c, taxis, clients, clientTaxiDictionary, clientMapLocation, clientLock
    # And lastly, for each taxi that isn't in the database, we add it
    try:
        for taxi in internalMemory:
            if taxi not in taxis:
                item = internalMemory[taxi]
                if isinstance(item, Taxi):
                    if item.client != '-':
                        clientTaxiDictionary.update({item.client: taxi})
                        if item.mounted == 'Y':
                            destination = internalMemory[item.client].destination
                        else:
                            destination = item.client    
                    else:
                        destination = '-'
                    c.execute(f"INSERT INTO Taxis VALUES ('{item.id}', '{destination}', '{item.state}'," + 
                            f"'{item.onService}', '{item.client}', '{item.active}', '{item.mounted}', '{item.pos}')")
                    
        for client in internalMemory:
            if client not in clients:
                item = internalMemory[client]
                if isinstance(item, Client):
                    destination = '-'
                    if item.id in clientMapLocation:
                        destination = clientMapLocation[item.id]
                    c.execute(f"INSERT INTO Clients VALUES ('{item.id}', '{item.destination}', '{item.state}', '{clientTaxiDictionary[client]}', '{destination}')")
    except Exception as e:
        print(f'[DATABASE MANAGER] THERE HAS BEEN AN ERROR WHILE ADDING TO THE DATABASE TABLES. {e}')

# DESCRIPTION: Este método lo que hace es actualizar la base de datos en base a la memoria interna
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def refresh():
    global dbLock, internalMemory, memLock, taxis, clients, clientTaxiDictionary, c, clientMapLocation, locLock

    try:
        while True:

            print('[DATABASE MANAGER] Updating database...')
            # We first connect to the database
            dbLock.acquire()
            conn = sqlite3.connect(DATABASE)
            # After that, we define the cursos to handle the db
            c = conn.cursor()

            # Then we get each id from the Taxis
            c.execute(f"SELECT id FROM Taxis")
            taxis = [item[0] for item in c.fetchall()]
            # and Clients
            c.execute(f"SELECT id FROM Clients")
            clients = [item[0] for item in c.fetchall()]

            memLock.acquire()
            #### DELETION PROCESS ####
            refreshDelete()

            # We create a dictionary to make it easier to track the clients' taxis
            clientTaxiDictionary = {}        

            #### UPDATE PROCESS ####
            refreshUpdate()

            #### ADDING PROCESS ####
            refreshAdd()

            memLock.release()

            # We commit the changes
            conn.commit()
            conn.close()
            dbLock.release()
            print('[DATABASE MANAGER] Database update is finished without errors')
            time.sleep(5)

    except Exception as e:
        print(f'[DATABASE MANAGER] THERE HAS BEEN AN ERROR WHILE UPDATING THE DATABASE TABLES. {e}')

# DESCRIPTION: Este método lo que hace es comprobar que existan la tablas Taxis y Clients en la base de datos,
# y en caso de que no existan, se crean
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def checkTablesForDB():
    global dbLock

    try:
        # We first connect to the database
        dbLock.acquire()
        conn = sqlite3.connect(DATABASE)
        # After that, we create the cursor to handle the database
        c = conn.cursor()

        # We check that each table in TABLENAMES exists
        for name in TABLENAMES:
            c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'")
            exists = c.fetchone()
            # In case it exists, we print that the table already exists,
            if exists:
                print(f'[DATABASE HANDLER] Table {name} found')
            # but in case it doesn't, we create the table
            else:
                print(f'[DATABASE HANDLER] Table {name} not found. Proceeding to table creation...')
                if name == 'Taxis':
                    c.execute("CREATE TABLE Taxis (id text PRIMARY KEY, destination text, state text, service text," + 
                              "client text, active text, mounted text, pos text)")
                elif name == 'Clients':
                    c.execute("CREATE TABLE Clients (id text PRIMARY KEY, destination text, state text, taxi text, pos text)")
                elif name == 'Registry':
                    c.execute("CREATE TABLE Registry (id text PRIMARY KEY, password text NOT NULL)")
        # We commit the changes done to the database
        conn.commit()
        # Then we close the database
        conn.close()
        dbLock.release()
    except Exception as e:
        print(f'[DATABASE MANAGER] THERE HAS BEEN AN ERROR WHILE GETTING THE DATABASE TABLES. {e}')
        conn.close()

# DESCRIPTION: Este método busca si un taxi está registrado en la memoria interna. De ser así, devuelve True, si no, devuelve False
# STARTING_VALUES: la id del taxi
# RETURNS: True or False
# NEEDS: NONE
def findTaxiID(taxiID):
    global internalMemory, memLock
    try:
        memLock.acquire()
        # We check whether the dictionary has the key (taxiID)
        condition = taxiID in internalMemory
        memLock.release()
        # And return the value
        return condition
    except Exception as e:
        print(f'[TAXI FLEET MANAGEMENT] THERE HAS BEEN AN ERROR WHILE FINDING TAXI ID ON DATABASE. {e}')

# DESCRIPTION: Este método encuentra si hay algún taxi que no tenga cliente y no esté onService. De ser así, lo devuelve, de no ser así, devuelve None
# STARTING_VALUES: NONE
# RETURNS: string con la id del taxi o None
# NEEDS: NONE
def getFirstAvailableTaxi():
    global internalMemory, memLock
    memLock.acquire()
    try:
        # We iterate through the keys in the internal dictionary and grab the value (item)
        for key in internalMemory:
            taxi = internalMemory[key]
            if isinstance(taxi, Client):
                continue
            # If it has no client assigned and is not on service, we return the key
            if taxi.client == '-' and taxi.onService == 'N' and taxi.state == 'OK':
                memLock.release()
                return key
        # If we don't find a taxi that meets the requirements, we return None
        memLock.release()
        return None
    except Exception as e:
        print(f'[CLIENT MANAGER] THERE WAS AN ERROR WHILE GETTING AN AVAILABLE TAXI. {e}')

############ CLIENT COMMUNICATION ############

# DESCRIPTION: Este método se encarga de manejar las peticiones de servicio de los clientes
# STARTING_VALUES: dirección del broker, puerto del broker
# RETURNS: NONE
# NEEDS: getFirstAvailableTaxi, sendTaxiToLocation
def hearClientPetitions():
    global internalMemory, memLock, locationDictionary, locLock, clientMapLocation, clientLock
    try:
        # We create the producer and consumer that should hear and answer the client petitions
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        consumer = KafkaConsumer('Customer2CentralPetitions', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        ackConsumer = KafkaConsumer('Customer2CentralACK', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT), consumer_timeout_ms=10000)
        # We then begin the consumer
        for message in consumer:
            decodedMessage = message.value.decode(FORMAT)
            # We detect it's a petition
            if "ASK" in decodedMessage:
                # We get the client ID
                clientID = decodedMessage[0:1]
                print(f'[CLIENT MANAGER] Client {clientID} asking for taxi service')
                # We get the first taxi available
                taxiForUser = getFirstAvailableTaxi()
                # There was no taxi available
                if taxiForUser is None:
                    # We send the petition response
                    threading.Timer(1, lambda: producer.send('Central2CustomerInformation', (clientID + '_KO').encode(FORMAT))).start()
                    # Adding client to pending ACK dictionary
                    addToClientConnections(clientID)
                    print(f'[CLIENT MANAGER] Rejected petition from client {clientID}')
                    # We receive the ACK
                    answer = False
                    for ackMessage in ackConsumer:
                        decodedACKMessage = ackMessage.value.decode(FORMAT)
                        if clientID + '_ACK' in decodedACKMessage:
                            # Deleting client from pending ACK dictionary
                            removeFromClientConnections(clientID)
                            answer = True
                            break
                    if answer:
                        continue
                    # RESILIENCE
                    print(f'[CLIENT MANAGER] Client {clientID} hasn\'n sent ACK. Disconnecting client...')
                # There was a taxi available
                else:
                    # We get where is the client (000)
                    clientLocation = decodedMessage[6:9]
                    clientDestination = decodedMessage[10:11]
                    # Translate the destination to the central format (A -> 000)
                    locLock.acquire()
                    clientDestination = locationDictionary[clientDestination]
                    locLock.release()
                    # We send the petition response
                    threading.Timer(1, lambda: producer.send('Central2CustomerInformation', (clientID + '_OK').encode(FORMAT))).start()
                    # Adding client to pending ACK dictionary
                    addToClientConnections(clientID)
                    # We receive the ACK
                    answer = False
                    for ackMessage in ackConsumer:
                        decodedACKMessage = ackMessage.value.decode(FORMAT)
                        if clientID + '_ACK' in decodedACKMessage:
                            # Deleting client from pending ACK dictionary
                            removeFromClientConnections(clientID)
                            # If we received the correct ACK, we assign the client to the taxi in the internal memory, as well as adding the client to the
                            # internal memory
                            memLock.acquire()
                            clientLock.acquire()
                            # We update the taxi in the internal memory
                            oldTaxiValue = internalMemory[taxiForUser]
                            newTaxiValue = Taxi(oldTaxiValue.id, oldTaxiValue.state, oldTaxiValue.active, "Y", clientID, oldTaxiValue.mounted, oldTaxiValue.pos)
                            internalMemory.update({taxiForUser: newTaxiValue})
                            # We add the client
                            newClientValue = Client(clientID, clientDestination,  'OK')
                            internalMemory.update({clientID: newClientValue})
                            # We add the client position
                            clientMapLocation.update({newClientValue.id: clientLocation})
                            memLock.release()
                            clientLock.release()
                            # We then send the taxi to the location where the user is
                            print(f'[CLIENT MANAGER] Sending taxi {taxiForUser} to client {clientID}')
                            sendTaxiToLocation(clientLocation, taxiForUser)
                            answer = True
                            break
                    if answer:
                        continue
                    # RESILIENCE
                    print(f'[CLIENT MANAGER] Client {clientID} hasn\'n sent ACK. Disconnecting client...')
                    
    except Exception as e:
        print(f'[CLIENT MANAGER] THERE WAS AN ERROR WHILE HANDLING A CLIENT PETITION. {e}')
    finally:
        producer.close()
        consumer.close()
        ackConsumer.close()

# DESCRIPTION: Este método se encarga de controlar que cuando el taxi llegue a la localización del cliente, la central lo avise,
# y que cuando el taxi llega al destino del cliente, este lo avise de nuevo
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: sendTaxiToLocation, informTaxiAboutMountOrDismount
def informClientAboutJourney():
    global internalMemory, memLock, locationDictionary, locLock, clientMapLocation
    try:
        # We create the Kafka consumer and producers so it can hear when the taxi is at the location or destination
        taxiConsumer = KafkaConsumer('DE2CentralService', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        clientConsumer = KafkaConsumer('Customer2CentralACK', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT), consumer_timeout_ms=3000)
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        # We hear the message from the taxi
        for message in taxiConsumer:
            decodedMessage = message.value.decode(FORMAT)
            # If it was a LOCATION message
            if 'LOCATION' in decodedMessage:
                taxiID = decodedMessage[0:2]
                time.sleep(1)
                producer.send('Central2DEServiceACK', (taxiID + '_ACK').encode(FORMAT))
                # We make sure the taxi had a client so we don't confuse it as a response for a CENTRAL CONTROL order
                memLock.acquire()
                value = internalMemory[taxiID]
                memLock.release()
                if value.client != '-':
                    # If the taxi had a client, we first send a MOUNT to the client
                    time.sleep(1)
                    producer.send('Central2CustomerInformation', (value.client + '_MOUNT').encode(FORMAT))
                    # Adding client to pending ACK dictionary
                    addToClientConnections(value.client)
                    print(f'[CLIENT MANAGER] Sending MOUNT message to client {value.client}')
                    # We create a boolean to check if the client has answered us
                    answer = False
                    for ACKMessage in clientConsumer:
                        decodedACKMessage = ACKMessage.value.decode(FORMAT)
                        if value.client in decodedACKMessage:
                            # Deleting client from pending ACK dictionary
                            removeFromClientConnections(value.client)
                            # If the client answers us with an ACK, we then inform the taxi of all necessary changes, and send him to the destination
                            informTaxiAboutMountOrDismount('mount', taxiID)
                            memLock.acquire()
                            locLock.acquire()
                            clientLock.acquire()
                            destination = internalMemory[value.client].destination
                            clientMapLocation.pop(value.client)
                            locLock.release()
                            memLock.release()
                            clientLock.release()
                            sendTaxiToLocation(destination, taxiID)
                            answer = True
                            break
                    # If the client has answered us, we skip the resilience bit
                    if answer == True:
                        continue
                    # RESILIENCE
            # If it was a DESTINATION message
            elif 'DESTINATION' in decodedMessage:
                taxiID = decodedMessage[0:2]
                time.sleep(1)
                producer.send('Central2DEServiceACK', (taxiID + '_ACK').encode(FORMAT))
                # We proceed to modify the internal memory to reflect the end of service for the taxi, as well as deleting the client
                time.sleep(0.5)
                memLock.acquire()
                locLock.acquire()
                value = internalMemory[taxiID]
                position = internalMemory[value.client].destination
                locLock.release()
                memLock.release()
                # We send the DISMOUNT message to the client
                time.sleep(1)
                producer.send('Central2CustomerInformation', (value.client + '_FINISHED_' + str(position)).encode(FORMAT))
                # Then we unassign the client from the taxi,
                informTaxiAboutMountOrDismount('dismount', taxiID)
                # as well as deleting the client from internal memory
                memLock.acquire()
                internalMemory.pop(value.client)
                memLock.release()
                print(f'[CLIENT MANAGER] Client {value.client} has reach its destination and has been deleted from database')
                # We create a boolean to check if the client has answered us
                answer = False
                for ACKMessage in clientConsumer:
                    decodedACKMessage = ACKMessage.value.decode(FORMAT)
                    if value.client in decodedACKMessage:
                        answer = True
                        break
                 # If the client has answered us, we skip the resilience bit
                if answer == True:
                    continue           
                # RESILIENCE
                print(f'[CLIENT MANAGER] Client {value.client} hasn\'t answered FINISHED message')
                # removeFromClientConnections(value.client)     

    except Exception as e:
        print(f'[CLIENT HANDLER] THERE HAS BEEN AN ERROR WHILE KEEPING TRACK OF THE CLIENTS\' JOURNEYS. {e}')
    finally:
        taxiConsumer.close()
        clientConsumer.close()
        producer.close()

# DESCRIPTION: Elimina el cliente de la memoria interna (internalMemory) y
# lo desasigna del taxi al que estuviera asignado
# STARTING_VALUES: Id del cliente
# RETURNS: NONE
# NEEDS: NONE
def removeFromClients(clientID):
    global memLock, internalMemory
    memLock.acquire()
    internalMemory.pop(clientID)
    for key, item in internalMemory.items():
        if isinstance(item, Taxi):
            if item.client == clientID:
                oldTaxiValue = internalMemory[key]
                newTaxiValue = Taxi(oldTaxiValue.id, oldTaxiValue.state, "N", "N", "-", "N", oldTaxiValue.pos)
                internalMemory.update({key: newTaxiValue})
    memLock.release()

# DESCRIPTION: Añade el cliente del diccionario de clientes y momento de envío del último mensaje
# STARTING_VALUES: Id del cliente
# RETURNS: NONE
# NEEDS: NONE
def addToClientConnections(clientID):
    global clientConnectionsLock, clientConnections
    clientConnectionsLock.acquire()
    clientConnections.update({str(clientID): time.time()})
    clientConnectionsLock.release()

# DESCRIPTION: Elimina el cliente del diccionario de clientes y momento de envío del último mensaje
# STARTING_VALUES: Id del cliente
# RETURNS: NONE
# NEEDS: NONE
def removeFromClientConnections(clientID):
    global clientConnectionsLock, clientConnections
    clientConnectionsLock.acquire()
    if str(clientID) in clientConnections.keys():
        clientConnections.pop(clientID)
    clientConnectionsLock.release()

# DESCRIPTION: Verifica cada segundo que no haya pasado más de SECONDS segundos entre el
# el envío de un mensaje al cliente y su respuesta
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: removeFromClients()
def checkClientConnections():
    global memLock, internalMemory, clientConnections, clientConnectionsLock
    while True:
        time.sleep(1)
        clientConnectionsLock.acquire()
        for key, item in clientConnections.items():
            if time.time() - item > SECONDS:
                print(f"[CLIENT MANAGER] CLIENT {str(key)} HASN'T ANSWERED FOR MORE THAN {SECONDS} SECONDS")            
                print(f"[CLIENT MANAGER] DELETING CLIENT {str(key)} FROM OUR DATABASE")
                # Deleting it from internal memory and pending answers
                removeFromClients(str(key))
                clientConnections.pop(key)
                break
        clientConnectionsLock.release()
        
            
############ TAXI COMMUNICATION ############

def generateBroadcastCertificate():
    try:

        broadCastCertificate = Fernet.generate_key()
        filePath = CERTIFICATE_FOLDER + "/Broadcast.cert"
        f = open(filePath, "w")
        f.write(broadCastCertificate.decode(FORMAT))
        f.close()
        print(f"[BROADCAST CERITIFICATE GENERATOR] Broadcast certificate generated successfully")
        print(broadCastCertificate)
    except Exception as e:
        print(f"[BROADCAST CERITIFICATE GENERATOR] THERE HAS BEEN AN ERROR GENERATING BROADCAST CERTIFICATE. {e}")
        return False


# DESCRIPTION: Este método recibe el ID de un taxi y obtiene, si existe,
# el token asociado a la sesión actual de dicho taxi.
# STARTING_VALUES: ID del taxi cuyo token se desea obtener
# RETURNS: Token de la sesión del taxi o False si no se ha encontrado
# NEEDS: NONE
def getToken(taxiID):
    global taxiSession
    
    for key, item in taxiSessions.items():
        if str(item[1]) == str(taxiID):
            return str(key)
    return False


def getCertificateFromFile(filename):
    try:
        filePath = CERTIFICATE_FOLDER+"/"+filename
        f = open(filePath, "r")
        certificate = f.readline().encode(FORMAT)
        f.close()
        return certificate
    except Exception as e:
        print(f"[CERTIFICATE HANDLER] THERE HAS BEEN AN ERROR READING THE CERTIFICATE. {e}")


# DESCRIPTION: Este método recibe un mensaje y la id del taxi al que está dirigido
# y devuelve el mensaje codificado en el formato token_mensajeCifrado, siendo este
# mensaje codificado con el certificado simétrico asociado a la sesión del taxi
# STARTING_VALUES: ID del taxi destino; Mensaje a codificar
# RETURNS: mensaje codificado en el formato token_mensajeCifrado o False si no se ha encontrado el id
# NEEDS: getToken()
def encodeMessage(taxiID=False, originalMessage="", broadcastEncoding=False):
    global taxiSession
    try:    
        certificate = False
        token = False
        if broadcastEncoding:
            certificate = getCertificateFromFile("Broadcast.cert")
            token = "broadcastMessage"
        else:
            taxiToken = getToken(str(taxiID).zfill(2))
            if taxiToken:
                token = taxiToken
                certificate = taxiSessions[taxiToken][0]
        
        if certificate:
            f = Fernet(certificate)
            # print(originalMessage)
            encodedMessage = f.encrypt(originalMessage.encode(FORMAT))
            finalMessage = token + "|" + encodedMessage.decode(FORMAT)
            print(f"[MESSAGE ENCODER] Message '{originalMessage}' encoded correctly")
            return finalMessage.encode(FORMAT)      # Como cadena de bytes
        
        return False
    except Exception as e:
        print(f"[MESSAGE ENCODER] THERE HAS BEEN AN ERROR ENCODING THE MESSAGE. {e}")
        return False

# DESCRIPTION: Este método recibe un mensaje codificado en el formato token_mensajeCifrado y devuelve el
# mensaje descifrado con el certificado simétrico asociado a la sesión del taxi
# STARTING_VALUES: Mensaje codificado en el formato token_mensajeCifrado (como cadena de caracteres)
# RETURNS: mensaje descifrado con el certificado simétrico asociado a la sesión del taxi
# NEEDS: NONE
def decodeMessage(message):
    global taxiSessions
    try:
        message = message.decode(FORMAT)
        splitMessage = message.split("|")
        # print("-------> MENSAJE RECIBIDO: " + message)
        taxiToken = splitMessage[0]
        encryptedMessage = splitMessage[1]
        taxiInfo = taxiSessions[taxiToken]
        if taxiInfo:
            taxiCertificate = taxiInfo[0]
            print("CERTIFICADO:")
            print(taxiCertificate)
            f = Fernet(taxiCertificate)
            # print(f"Taxi certificate used for decoding: {taxiCertificate}")
            # print(f"Mensaje a desencriptar: {encryptedMessage}")
            originalMessage = f.decrypt(encryptedMessage.encode(FORMAT))
            print(f"[MESSAGE DECODER] Message '{originalMessage.decode(FORMAT)}' decoded correctly")
            # print(originalMessage.decode(FORMAT))
            return originalMessage.decode(FORMAT)       # Devuelve el mensaje codificado como cadena de caracteres
    
    except Exception as e:
        print(f"[MESSAGE DECODER] THERE HAS BEEN AN ERROR DECODING THE MESSAGE. {e}")
        return False
    
# DESCRIPTION: Este método lo que hace es enviar un string con la información del mapa a los taxis
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def sendMapToTaxis():
    global internalMemory, memLock, clientMapLocation, clientLock, locationDictionary, locLock

    try:
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        while True:
            time.sleep(1)
            # We define the empty dictionaries
            taxis = {}
            clients = {}
            locations = {}
            # Then we get each taxi,
            memLock.acquire()
            for key in internalMemory:
                item = internalMemory[key]
                if isinstance(item, Taxi):
                    taxis.update({item.id: item})
            memLock.release()
            # each client,
            clientLock.acquire()
            for key in clientMapLocation:
                item = clientMapLocation[key]
                clients.update({key: item})
            clientLock.release()
            # and each location
            locLock.acquire()
            for key in locationDictionary:
                item = locationDictionary[key]
                locations.update({key: item})
            locLock.release()

            # Then we build the string to send
            mapString = ''
            for i in range(MAP_COLUMNS*MAP_ROWS):
                character = '#'
                for key in locations:
                    if i == int(locations[key]):
                        character = key
                        break
                for key in clients:
                    if i == int(clients[key]):
                        character = key
                        break
                for key in taxis:
                    if i == int(taxis[key].pos):
                        character = key
                        if taxis[key].mounted == 'Y':
                            character += taxis[key].client
                        if taxis[key].state == 'KO':
                            character += '!'
                        if taxis[key].active == 'N':
                            character += '-'
                mapString += character
                if i != (MAP_COLUMNS*MAP_ROWS - 1):
                    mapString += ','
            # And send the message
            encodedMapMessage = encodeMessage(originalMessage=mapString, broadcastEncoding=True)
            producer.send('Central2DEMap', encodedMapMessage)

    except Exception as e:
        print(f'[MAP TO TAXI SENDER] THERE HAS BEEN AN ERROR WHILE SENDING THE MAP SITUATION TO THE TAXIS. {e}')
    finally:
        producer.close()

# DESCRIPTION: Este método lo que hace es verificar si la ID de taxi ya existe. En caso de que sí, rechaza la conexión,
# pero si no, da de alta en la base de datos, al taxi. En cualquier caso, informa a este de la decisión tomada
# y en caso de que no existan, se crean
# STARTING_VALUES: conexión con el taxi, la dirección del taxi
# RETURNS: NONE
# NEEDS: findID()
def authenticate(conn, addr):
    global internalMemory, memLock, connDictionary, connDicLock
    authenticationOK = False

    conn.settimeout(5)
    print(f"[TAXI AUTHENTICATION SERVICE]: Stablished connection with taxi on {addr}")                  
    while True:
        try:
            # Receive taxi information (Format: id_password_auth/reauth)
            taxiInfo = conn.recv(HEADER).decode(FORMAT)
            # Prepare taxi information for future use
            processedInfo = taxiInfo.split("_")
            taxiId = processedInfo[0]
            passwd = processedInfo[1]
            authType = processedInfo[2]
            
            print(f"[TAXI AUTHENTICATION SERVICE]: Received taxi authentication information for taxi {taxiId}")

            # If taxi is asking for a new session
            if authType == "auth":
                # Check if taxi is registered
                if findInRegistry(taxiId, passwd):
                    # Check if the taxi has an active session
                    if not findTaxiID(taxiId):
                        authenticationOK = True
                        # And add it to the internal memory (dictionary)
                        memLock.acquire()
                        newValue = Taxi(taxiId, 'OK', 'N', 'N', '-', 'N', '000')
                        internalMemory.update({taxiId: newValue})
                        memLock.release()
                        # As well as creating its disconnection counter
                        connDicLock.acquire()
                        connDictionary.update({newValue.id: 0})
                        connDicLock.release()

            # If taxi is asking for recovering a lost session
            else:           # (reauth)
                # Check if the taxi has an active session
                if findTaxiID(taxiId):
                    authenticationOK = True

            # If the taxi accomplishes the requirements for starting a session
            if authenticationOK:
                token, certificate = updateSessions(taxiId)
                broadcastCertificate = getCertificateFromFile("Broadcast.cert")
                message = f"OK|{token}|{certificate.decode(FORMAT)}|{broadcastCertificate.decode(FORMAT)}"        # Convertimos la clave en una cadena de caracteres
                print(f"------------------> BORRAR: CLAVE GENERADA = {certificate}")
                # message = ("OK_" + str(token) + "_").encode(FORMAT)
                # message = message + certificate

                conn.send(message.encode(FORMAT))
                print(f"[TAXI AUTHENTICATION SERVICE]: Completed authentication process for {taxiId}")
                break

            else:
                conn.send("KO".encode(FORMAT))
                print(f"[TAXI AUTHENTICATION SERVICE]: Taxi authentication rejected for taxi {taxiId}")
                break
                
        except socket.timeout:
            print("[TAXI AUTHENTICATION SERVICE]: LOST connection with taxi")
            conn.close()                    
            break
        
        except Exception as e:
            print(f"[TAXI AUTHENTICATION SERVICE]: THERE HAS BEEN AN ERROR ON AUTHENTICATION OF {addr}. {e}")
            conn.close()
            break

# DESCRIPTION: Método que genera y almacena en memoria el token y el certificado para la sesión del taxi
# STARTING_VALUES: ID del taxi que va a iniciar sesión
# RETURNS: NONE
# NEEDS: NONE
def updateSessions(taxiID):
    global taxiSessions
    try:
        token = token_hex(16)
        certificate = Fernet.generate_key()
        print(f"[SESSION MANAGER] Generated session token and certificate for {taxiID}")
    except Exception as e:
        print(f"[SESSION MANAGER] THERE HAS BEEN AN ERROR ON TOKEN OR CERTIFICATE CREATION FOR TAXI {taxiID}. {e}")
    try:
        taxiSessions[token] = (certificate, taxiID)
        print(f"[SESSION MANAGER] Stored session token and certificate for {taxiID}")
    except Exception as e:
        print(f"[SESSION MANAGER] THERE HAS BEEN AN ERROR ON ACTIVE SESSIONS REGISTRY FOR {taxiID}. {e}")

    return token, certificate

# DESCRIPTION: Este método lo que hace es esperar alguna petición de algun taxi para la conexión,
# y en caso de recibirlo, abre un nuevo hilo con la conexión de ese socket
# STARTING_VALUES: dirección ip de la central, puerto que utilizará para escuchar las peticiones
# RETURNS: NONE
# NEEDS: authenticate()
def startAuthenticationService(SERVER, PORT):
    try:
        #We first create the socket,
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # then bind it to the ip address and port,
        server.bind((SERVER, PORT))

        print("[TAXI AUTHENTICATION SERVICE]: Initializing server for taxi registration...")
        # then, begin listening
        server.listen()
        print(f"[TAXI AUTHENTICATION SERVICE]: Server active on {SERVER}:{PORT} ...")
        
        while True:
            try:
                client_socket, addr = server.accept()
                conn = ssl.wrap_socket(client_socket, server_side=True, 
		                                certfile="./CentralCertificate/server.crt", keyfile="./CentralCertificate/server.key",
		                                ssl_version=ssl.PROTOCOL_TLSv1)
                thread = threading.Thread(target=authenticate, args=(conn, addr))
                thread.daemon = True
                thread.start()
            except Exception as e:
                print(f"[TAXI AUTHENTICATION SERVICE]: THERE HAS BEEN AN ERROR WHILE STABLISHING CONNECTION WITH {addr}. {e}")
                conn.close()
    except Exception as e:
        print(f"[TAXI AUTHENTICATION SERVICE]: THERE HAS BEEN AN ERROR WHILE OPENING THE AUTHENTICATION SERVER")

# DESCRIPTION: Este método lo que hace es recibir el estado de los taxis.
# STARTING_VALUES: la dirección del broker, y el puerto del broker
# RETURNS: NONE
# NEEDS: NONE
def hearTaxiStates():
    global internalMemory, memLock, connDictionary, connDicLock
    try:
        # We create the kafka consumer and produce in charge of listening the message sent by the taxi, and sending the ACK
        consumer = KafkaConsumer('DE2CentralState', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        # Then we hear the received message
        for message in consumer:
            # We decode the message and extract its information
            # <TAXIID>_<LOCATION>_<STATE>_<ACTIVE>_<MOUNTED>
            decodedMessage = decodeMessage(message.value)
            # If sender's token is correct
            if decodedMessage:
                taxiID = decodedMessage[0:2]
                location = decodedMessage[3:6]
                state = decodedMessage[7:9]
                active = decodedMessage[10:11]
                mounted = decodedMessage[12:]
                # We then create a thread that after a second, will send the ACK through kafka
                message = ("ACK_" + taxiID)
                encodedMessage = encodeMessage(taxiID, message)
                threading.Timer(1, lambda: producer.send('Central2DEACK', encodedMessage)).start()
                # Then we update the internalMemory (dictionary) for the taxi, first creating a new Taxi entity
                memLock.acquire()
                oldValue = internalMemory[taxiID]
                newValue = Taxi(taxiID, state, active, oldValue.onService, oldValue.client, mounted, location)
                internalMemory.update({taxiID: newValue})
                memLock.release()
                # As well as updating the disconnection counter
                connDicLock.acquire()
                connDictionary.update({taxiID: 0})
                connDicLock.release()

    except Exception as e:
        print(f"[TAXI FLEET MANAGEMENT]: THERE HAS BEEN AN ERROR WHILE HEARING TAXIS' STATES. {e}")
    finally:
        consumer.close()
        producer.close()

# DESCRIPTION: Este método lo que hace es aumentar el contador de desconexión de los taxis cada segundo. Cuando llegue a diez, este borrará al taxi de la memoria interna
# y la base de datos.
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: getFirstAvailableTaxi(), sendTaxiToLocation()
def manageTaxiDisconnection():
    global internalMemory, memLock, connDictionary, connDicLock, clientMapLocation, clientLock
    try:
        while True:
            time.sleep(1)
            # This is the list where we will keep the key to delete from the internal memory and connection dictionary
            ls = []
            # We get the disconnection semaphore
            connDicLock.acquire()
            # Then for each key in the dictionary
            for key in connDictionary:
                # We uptick the value by one
                connDictionary.update({key: connDictionary[key] + 1})
                # In case it's 10 or more, we delete them from the dicionary and the internal memory
                if connDictionary[key] >= 10:
                    ls.append(key)

            # We get the internal memory semaphore
            memLock.acquire()  

            for item in ls:
                # We get the taxi object to check certain values
                taxi = internalMemory[item]
                # In case the taxi is mounted
                if taxi.mounted == 'Y':
                    # We get the client and current position of the taxi.
                    client = taxi.client
                    position = taxi.pos
                    # We then inform the client, and then delete it from the internal memory
                    producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
                    producer.send('Central2CustomerInformation', (str(client) + '_BROKE_' + str(position)).encode(FORMAT))
                    producer.close()
                    internalMemory.pop(client)
                elif taxi.client != '-':
                    # We get the client location semaphore
                    clientLock.acquire()
                    # We get the clients location
                    location = clientMapLocation[taxi.client]
                    # and the destination
                    destination = internalMemory[taxi.client].destination
                    # We release the client location semaphore
                    clientLock.release()
                    memLock.release()
                    # Then we get another taxi for the client
                    taxiForUser = getFirstAvailableTaxi()
                    memLock.acquire()
                    # If there is none, we send a BROKE message to the client
                    if taxiForUser is None:
                        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
                        producer.send('Central2CustomerInformation', (str(taxi.client) + '_BROKE_' + str(destination)).encode(FORMAT))
                        producer.close()
                        internalMemory.pop(taxi.client)
                    else:
                        oldTaxiValue = internalMemory[taxiForUser]
                        newTaxiValue = Taxi(oldTaxiValue.id, oldTaxiValue.state, oldTaxiValue.active, "Y", taxi.client, oldTaxiValue.mounted, oldTaxiValue.pos)
                        internalMemory.update({taxiForUser: newTaxiValue})
                        sendTaxiToLocation(str(location), str(taxiForUser))
                # We delete it from the internal memory
                internalMemory.pop(item)
                # We delete it from the disconnection dictionary
                connDictionary.pop(item)
                print(f'[DISCONNECTION HANDLER] Taxi {item} has been deleted from the database due to no connection')

            # We release the internal memory semaphore
            memLock.release()
            # We release the disconnection semaphore
            connDicLock.release()
    except Exception as e:
        print(f'[DISCONNECTION HANDLER] THERE HAS BEEN AN ERROR WHILE KEEPING TRACK OF THE CONNECTIONS. {e}')

# DESCRIPTION: Este método lo que hace es informar al taxi sobre si el cliente se monta, o desmonta
# STARTING_VALUES: string con "mount" o "dismount", id del taxi, la dirección del broker, el puerto del broker
# RETURNS: True or False
# NEEDS: NONE
def informTaxiAboutMountOrDismount(string, taxiID):
    global internalMemory, memLock
    try:
        # We create the kafka consumer and producer in charge of informing the taxi about the client mount, and hearing its ACK
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT)) 
        consumer = KafkaConsumer('DE2CentralACK', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT), consumer_timeout_ms=10000)     
        # Then we send, depending on the string, mount or dismount
        if string == "mount":
            encodedOrderMessage = encodeMessage(taxiID, f"{taxiID}_MOUNT")
            producer.send('Central2DEOrder', encodedOrderMessage)
        elif string == "dismount":
            encodedOrderMessage = encodeMessage(taxiID, f"{taxiID}_DISMOUNT")
            producer.send('Central2DEOrder', encodedOrderMessage)
        # Then we hear the ACK
        for message in consumer:
            decodedMessage = decodeMessage(message.value)
            # decodedMessage = message.value.decode(FORMAT)
            if decodedMessage:
                if taxiID in decodedMessage:
                    if 'ACK' in decodedMessage:
                        memLock.acquire()
                        # If we want to mount the client in the taxi, we set the mount value in the dictionary to 'Y'
                        if string == "mount":
                            oldValue = internalMemory[taxiID]
                            newValue = Taxi(oldValue.id, oldValue.state, oldValue.active, oldValue.onService, oldValue.client, 'Y', oldValue.pos)
                            internalMemory.update({taxiID: newValue})
                        # But if we want to dismount the client in the taxi, we set the mount value in the dictionary to 'N', and we dissasociate the client
                        elif string == "dismount":
                            oldValue = internalMemory[taxiID]
                            newValue = Taxi(oldValue.id, oldValue.state, 'N', 'N', '-', 'N', oldValue.pos)
                            internalMemory.update({taxiID: newValue})
                        memLock.release()
                        return True
        print(f'[CENTRAL CONTROL] Taxi {taxiID} hasn\'t answered the mount/dismount petition. Proceeding with deletion...')
        # RESILIENCE
        return False   
    except Exception as e:
        print(f"[TAXI SERVICE MANAGEMENT]: THERE HAS BEEN AN ERROR WHILE INFORMING TAXI {taxiID} ABOUT THE CLIENT MOUNT. {e}")
    finally:
        producer.close()
        consumer.close()    

##### SENDING ORDERS #####

# DESCRIPTION: Este método lo que hace es enviar la orden de dirigirse a una
# localización concreta dentro del mapa a un taxi
# STARTING_VALUES: localización destino, taxi al que se envía
# RETURNS: NONE
# NEEDS: NONE
def sendTaxiToLocation(location, taxiID):
    try:
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        consumer = KafkaConsumer('DE2CentralACK', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT), consumer_timeout_ms = 5000)
        # Encriptación y envío del mensaje 
        print("------------------------------------Proceso de envío de orden: "+  (str(taxiID).zfill(2) + "_GO_" + str(location).zfill(3)))
        encodedOrderMessage = encodeMessage(taxiID, (str(taxiID).zfill(2) + "_GO_" + str(location).zfill(3)))
        print(f"------------------------------------Orden generada: {encodedOrderMessage} ")
        producer.send("Central2DEOrder", encodedOrderMessage)
        print(f'[SEND TAXI TO LOCATION] Sending taxi {taxiID} to location {str(location).zfill(3)}')
        # We make a boolean to know if it has answered us
        answer = False
        for message in consumer:
            decodedMessage = decodeMessage(message.value)
            if decodedMessage:
                if taxiID in decodedMessage:
                    answer = True
                    break
        if not answer:
            # RESILIENCE
            print(f'[SEND TAXI TO LOCATION] Taxi {taxiID} hasn\'t answered GO order. Proceeding with deletion...')
    except Exception as e:
        print(f"[TAXI SERVICE MANAGEMENT]: THERE HAS BEEN AN ERROR WHILE SENDING THE TAXI TO THE LOCATION. {e}")
    finally:
        producer.close()
        consumer.close()    

# DESCRIPTION: Este método lo que hace es enviar la orden de parar a un taxi
# STARTING_VALUES: taxi al que se envía
# RETURNS: NONE
# NEEDS: NONE
def stopTaxi():
    global selectedTaxi
    taxiID = str(selectedTaxi)
    try:
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        consumer = KafkaConsumer('DE2CentralACK', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        encodedOrderMessage = encodeMessage(taxiID, str(taxiID).zfill(2) + "_STOP")
        producer.send("Central2DEOrder", encodedOrderMessage)
        print(f'[STOP TAXI] Stopping taxi {taxiID}')
        # We make a boolean to know if it has answered us
        answer = False
        for message in consumer:
            decodedMessage = decodeMessage(message.value)
            if decodedMessage:
                if taxiID in decodedMessage:
                    answer = True
                    break
        if not answer:
            # RESILIENCE
            print(f'[STOP TAXI] Taxi {taxiID} hasn\'t answered STOP order. Proceeding with deletion...')

    except Exception as e:
        print(f"[TAXI CONTROL]: THERE HAS BEEN AN ERROR WHILE STOPPING THE TAXI {taxiID}. {e}")
    finally:
        producer.close()
        consumer.close()

# DESCRIPTION: Este método lo que hace es enviar la orden de parar a un taxi
# STARTING_VALUES: taxi al que se envía
# RETURNS: NONE
# NEEDS: NONE
def resumeTaxi():
    global selectedTaxi
    taxiID = str(selectedTaxi)
    try:
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        consumer = KafkaConsumer('DE2CentralACK', bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        encodedOrderMessage = encodeMessage(taxiID, (taxiID).zfill(2) + "_RESUME")
        producer.send("Central2DEOrder", encodedOrderMessage)
        print(f'[RESUME TAXI\'S WAY] Resuming taxi {taxiID} service')
        # We make a boolean to know if it has answered us
        answer = False
        for message in consumer:
            decodedMessage = decodedMessage(message.value)
            if decodedMessage:
                if taxiID in decodedMessage:
                    answer = True
                    break
        if not answer:
            # RESILIENCE
            print(f'[RESUME TAXI\'S WAY] Taxi {taxiID} hasn\'t answered RESUME order. Proceeding with deletion...')
    except Exception as e:
        print(f"[TAXI CONTROL]: THERE HAS BEEN AN ERROR WHILE RESUMING TAXI'S {taxiID} WAY. {e}")
    finally:
        producer.close()
        consumer.close()

# DESCRIPTION: Envía el taxi seleccionado a la posición de la base (000)
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: sendTaxiToLocation()
def goBase():
    global selectedTaxi
    taxiID = selectedTaxi
    sendTaxiToLocation("000", str(taxiID))

# DESCRIPTION: Envía el taxi seleccionado (selectedTaxi) a la posición seleccionada (selectedPos)
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: sendTaxiToLocation()
def goLocation():
    global selectedTaxi, selectedPos
    taxiID = selectedTaxi
    pos = selectedPos
    sendTaxiToLocation(str(pos), str(taxiID))




############ GRAPHICAL USER INTERFACE ############ 

# DESCRIPTION: Almacena en selectedPos la posición del mapa seleccionada
# STARTING_VALUES: Frame principal de la interfaz gráfica
# RETURNS: el mapa, en forma de matriz de botones
# NEEDS: NONE
def createMap(mainFrame):
    try:
        # Marco para el mapa
        mapFrame = ttk.LabelFrame(mainFrame, text="Mapa de la Ciudad (20x20)")
        mapFrame.grid(row=0, column=1, padx=10, pady=10)
        
        # Crear cuadrícula de 20x20
        mapButtons = []
        for i in range(20):
            row = []
            for j in range(20):
                # Crear botón para cada casilla
                btn = tk.Button(mapFrame, width=2, height=1, 
                                bg='white', 
                                command=lambda x=j, y=i: onMapClick(x, y))
                btn.grid(row=i, column=j, padx=1, pady=1)
                row.append(btn)
            mapButtons.append(row)
        return mapButtons
    except Exception as e:
        print(f"[GUI MAP CREATOR]: THERE HAS BEEN AN ERROR WHILE CREATING THE MAP. {e}")

# DESCRIPTION: Almacena en selectedPos la posición del mapa seleccionada
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def onMapClick(x, y):
        global selectedPos
        # Manejar clic en una casilla del mapa (para futuras expansiones)
        selectedPos = y * MAP_COLUMNS + x
        print(f"[GUI MANAGEMENT] Clicked on position: ({x}, {y}), sending to {selectedPos}")

# DESCRIPTION: Almacena en selectedTaxi el taxi seleccionado
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def selectItem(a):
    global selectedTaxi
    curItem = taxiTableGlobal.focus()
    print(f"[GUI MANAGEMENT] Selected item: {taxiTableGlobal.item(curItem)['values']}")
    selectedTaxi = taxiTableGlobal.item(curItem)['values'][0]

# DESCRIPTION: Crear las tablas en la pantalla
# STARTING_VALUES: frame principal de la pantalla
# RETURNS: NONE
# NEEDS: goBase(), goLocation(), resumeTaxi(), stopTaxi(), onMapClick()
def createTables(mainframe):
    global taxiTableGlobal
    try:
        # Frame para las tablas
        tablesFrame = ttk.Frame(mainFrame)
        tablesFrame.grid(row=0, column=0, padx=10, pady=10)
        
        ######################
        #   Tabla de taxis   #
        ######################

        # Tabla de taxis
        taxiFrame = ttk.LabelFrame(tablesFrame, text="Taxis")
        taxiFrame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        # Crear frame para la tabla y scrollbar
        taxiScroll = ttk.Frame(taxiFrame)
        taxiScroll.grid(row=0, column=0, sticky=(tk.W, tk.E))

        # Crear scrollbar
        scrollbar = ttk.Scrollbar(taxiScroll)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Crear tabla
        taxiTableGlobal = ttk.Treeview(taxiScroll, yscrollcommand=scrollbar.set)
        taxiTableGlobal['columns'] = ('ID', 'Destino', 'Estado')
        # Mostrar solo los encabezados (oculta la columna índice por defecto)
        taxiTableGlobal.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Configurar scrollbar
        scrollbar.config(command=taxiTableGlobal.yview)

        # Configurar columnas
        taxiTableGlobal['show'] = 'headings'
        taxiTableGlobal.heading('ID', text='ID')
        taxiTableGlobal.heading('Destino', text='Destino')
        taxiTableGlobal.heading('Estado', text='Estado')
        taxiTableGlobal.column('ID', width=80)
        taxiTableGlobal.column('Destino', width=80)
        taxiTableGlobal.column('Estado', width=150)
        taxiTableGlobal.bind('<ButtonRelease-1>', selectItem)
        
        #   Botones taxis   #
        #########################

        taxiButtonFrame = ttk.Frame(taxiFrame)
        taxiButtonFrame.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        ttk.Button(taxiButtonFrame, text="Parar", 
        command=stopTaxi).pack(side=tk.LEFT, padx=5)
        ttk.Button(taxiButtonFrame, text="Reanudar", 
        command=resumeTaxi).pack(side=tk.LEFT, padx=5)
        ttk.Button(taxiButtonFrame, text="Ir a destino", 
        command=goLocation).pack(side=tk.LEFT, padx=5)
        ttk.Button(taxiButtonFrame, text="Volver a base", 
        command=goBase).pack(side=tk.LEFT, padx=5)


        #########################
        #   Tabla de clientes   #
        #########################

        # Tabla de clientes
        clientFrame = ttk.LabelFrame(tablesFrame, text="Clientes")
        clientFrame.grid(row=2, column=0, sticky=(tk.W, tk.E))

        # Crear frame para la tabla y scrollbar
        clientScroll = ttk.Frame(clientFrame)
        clientScroll.grid(row=0, column=0, sticky=(tk.W, tk.E))

        # Crear scrollbar
        scrollbar = ttk.Scrollbar(clientScroll)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Crear tabla
        clientTable = ttk.Treeview(clientScroll, yscrollcommand=scrollbar.set)
        clientTable['columns'] = ('ID', 'Destino', 'Estado')
        # Mostrar solo los encabezados (oculta la columna índice por defecto)
        clientTable.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Configurar scrollbar
        scrollbar.config(command=clientTable.yview)

        # Configurar columnas
        clientTable['show'] = 'headings'
        clientTable.heading('ID', text='ID')
        clientTable.heading('Destino', text='Destino')
        clientTable.heading('Estado', text='Estado')
        clientTable.column('ID', width=80)
        clientTable.column('Destino', width=80)
        clientTable.column('Estado', width=150)

        clientFrame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(30,0))  # 10 píxeles de padding arriba
        return clientTable
    except Exception as e:
        print(f"[GUI TABLE CREATOR]: THERE HAS BEEN AN ERROR WHILE CREATING TABLES. {e}")

# DESCRIPTION: Actualiza el mapa en la pantalla
# STARTING_VALUES: mapa, en forma de matriz de botones
# RETURNS: NONE
# NEEDS: NONE
def updateMap(mapButtons):
    try:
        global internalMemory, memLock, clientMapLocation, clientLock
        # First clean the map
        for i in range(20):
            for j in range(20):
                mapButtons[i][j].configure(bg='white', text='')
        
        # Draw location on map
        locLock.acquire()
        for key, item in locationDictionary.items():
            y = int(item) % MAP_COLUMNS
            x = int(item) // MAP_COLUMNS
            mapButtons[x][y].configure(
                    bg="cyan",
                    text=str(key)
                )
        locLock.release()

        # Draw client on map
        for key, clientItem in clientMapLocation.items():
            # Position
            y = int(clientItem) % MAP_COLUMNS
            x = int(clientItem) // MAP_COLUMNS
            color = "yellow"
            mapButtons[x][y].configure(
                    bg=color,
                    text=key
                )    

        # Draw taxis on map
        memLock.acquire()
        for key, item in internalMemory.items():
            if isinstance(item, Taxi):
                # Different colors depending on state
                color = "green"  # Por defecto
                if str(item.active) == "N":
                    color = "red"
                # Creating text
                infoText = str(item.id)
                # Is it KO?
                if item.state == "KO":
                    infoText += "!"
                    color = "red"
                # Is a client on board?
                if item.mounted == "Y":
                    infoText += str(item.client)
                # Position
                y = int(item.pos) % MAP_COLUMNS
                x = int(item.pos) // MAP_COLUMNS
                mapButtons[x][y].configure(
                    bg=color,
                    text=infoText
                )
        memLock.release()


    except Exception as e:
        print(f"[GUI MAP UPDATER]: THERE HAS BEEN AN ERROR WHILE UPDATING THE MAP. {e}")

# DESCRIPTION: Actualiza las tablas en la pantalla
# STARTING_VALUES: tabla de cliente
# RETURNS: NONE
# NEEDS: NONE
def updateTables(clientTable):
    global internalMemory, memLock, locationDictionary, locLock
    try:
        # Clean tables
        for item in taxiTableGlobal.get_children():
            taxiTableGlobal.delete(item)
        for item in clientTable.get_children():
            clientTable.delete(item)
        
        # Actualizar tabla de taxis
        memLock.acquire()
        for key, item in internalMemory.items():
            if isinstance(item, Taxi):
                # Creating destination
                destination = "-"
                if item.active == "Y":
                    if item.mounted == "Y":
                        if str(item.client) in internalMemory:
                            locLock.acquire()
                            locFound = False
                            for key, locItem in locationDictionary.items():
                                if locItem == internalMemory[str(item.client)].destination:
                                    locFound = key
                            locLock.release()
                            if locFound:
                                destination = locFound
                            else:
                                destination = internalMemory[str(item.client)].destination
                    else:
                        destination = str(item.client)
                # Creating state
                infoState = item.state
                if item.active == "Y":
                    infoState = infoState + " Servicio " + item.client
                else:
                    infoState += " Parado"
                taxiTableGlobal.insert('', 'end', values=(
                    item.id,
                    destination,
                    infoState                    
                ))
        # Actualizar tabla de clientes
        for key, item in internalMemory.items():
            if isinstance(item, Client):
                destInfo = item.destination
                for key, locItem in locationDictionary.items():
                    if locItem == item.destination:
                        destInfo = key

                clientTable.insert('', 'end', values=(
                    item.id,
                    destInfo,
                    item.state
                ))
        memLock.release()
    except Exception as e:
        print(f"[GUI TABLE UPDATER]: THERE HAS BEEN AN ERROR WHILE UPDATING TABLES. {e}")

# DESCRIPTION: Actualiza los elementos variables de la interfaz gráfica
# STARTING_VALUES: mapa, en forma de matriz de botones
# RETURNS: NONE
# NEEDS: updateTables(), updateMap()
def updateGUI(clientTable, mapButtons, root):
    updateMap(mapButtons)
    updateTables(clientTable)
    root.after(1000, lambda: updateGUI(clientTable, mapButtons, root))

############ PROGRAM STARTING POINT ############ 

if  (len(sys.argv) == 5):
    # Argument management
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    BROKER_IP = sys.argv[3]
    BROKER_PORT = int(sys.argv[4])
    # Preparing data for future uses
    ADDR = (SERVER, PORT)               # Server address 

    selectedTaxi = None
    selectedPos = "000"
    weatherState = "GOOD"
    weatherState2 = "GOOD"

    # We initialize the internalMemory to an empty dictionary
    internalMemory = {}
    locationDictionary = {}
    clientMapLocation = {}
    connDictionary = {}
    clientConnections = {}
    taxiSessions = {}               # Dictitonary for active taxi sessions
    # We initialize the semaphores used
    dbLock = threading.Lock()
    memLock = threading.Lock()
    locLock = threading.Lock()
    clientLock = threading.Lock()
    clientConnectionsLock = threading.Lock()
    connDicLock = threading.Lock()
    
    try:    

        # We create the location dictionary
        readJsonConfigurationFile()
        # We check the database and create the tables if necessary
        checkTablesForDB()
        # We initialize the internalMemory with the DDBB data
        initializeInnerMemory()
        
        # Generate certificate for broadcast comunication
        generateBroadcastCertificate()

        # We creathe a thread to periodically check the condition of the city temperature
        temperatureThread = threading.Thread(target=weatherManager)
        temperatureThread.daemon = True
        temperatureThread.start()

        # We create a thread to begin listening for taxi authentification
        taxiAuthenticationThread = threading.Thread(target=startAuthenticationService, args=(ADDR[0], ADDR[1]))
        taxiAuthenticationThread.daemon = True
        taxiAuthenticationThread.start()

        # We create a thread to begin listening for taxi states
        hearTaxiStatesThread = threading.Thread(target=hearTaxiStates)
        hearTaxiStatesThread.daemon = True
        hearTaxiStatesThread.start()

                # We create a thread to begin listening for client petitions
        hearClientPetitionsThread = threading.Thread(target=hearClientPetitions)
        hearClientPetitionsThread.daemon = True
        hearClientPetitionsThread.start()

        # We create a thread to begin listening the LOCATION and DESTINATION messages
        hearLocationAndDestination = threading.Thread(target=informClientAboutJourney)
        hearLocationAndDestination.daemon = True
        hearLocationAndDestination.start()

        # We create a thread to begin checking all the clients who had been sent a message
        # and hasn't answered yet
        checkClientConnectionsThrad = threading.Thread(target=checkClientConnections)
        checkClientConnectionsThrad.daemon = True
        checkClientConnectionsThrad.start()

        # We create a thread that refreshes the database
        databaseThread = threading.Thread(target=refresh)
        databaseThread.daemon = True
        databaseThread.start()

        # We then create a thread that ticks the disconnection counter up for each taxi every second. It will be set to
        # 0 whenever it hears a state from the taxi
        disconnectionThread = threading.Thread(target=manageTaxiDisconnection)
        disconnectionThread.daemon = True
        disconnectionThread.start()

        # We create a thread that sends the map situation to the taxis
        sendMapThread = threading.Thread(target=sendMapToTaxis)
        sendMapThread.daemon = True
        sendMapThread.start()

        # Graphical User Interface
        root = tk.Tk()
        root.title("EasyCab Central Control Panel")
        mainFrame = ttk.Frame(root, padding="10")
        mainFrame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        # Create tables for taxi and customer
        clientTable = createTables(mainFrame)
        # Create map of size 20x20
        mapButtons = createMap(mainFrame)
        time.sleep(3)

        # # We create a thread to begin listening the LOCATION and DESTINATION messages
        # hearLocationAndDestination = threading.Thread(target=updateGUI, args=(taxiTable, clientTable, mapButtons))
        # hearLocationAndDestination.daemon = True
        # hearLocationAndDestination.start()
        
        root.after(1000, lambda: updateGUI(clientTable, mapButtons, root))

        root.mainloop() 
        # while True:
        #     root.update()
        #     updateGUI(taxiTable, clientTable, mapButtons)

    except KeyboardInterrupt:
        print('[CENTRAL CONTROL] Application shutdown due to human interaction')
    except Exception as e:
        print(f'[CENTRAL CONTROL] Application shutdown due to unexpected error. {e}')
    finally:
        print(f'EXITING CENTRAL APPLICATION')

else:
    print("Sorry, incorrect parameter use\n[USAGE <LISTENING IP> <LISTENING PORT> <BROKER IP> <BROKER PORT>]")