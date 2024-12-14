import sys
from flask import Flask, request, jsonify
import threading
import time
import socket
from kafka import KafkaProducer, KafkaConsumer
import tkinter as tk
from tkinter import ttk
from encodings import idna
import copy
import ssl
import requests
import hashlib
from cryptography.fernet import Fernet

MAP_ROWS = 20
MAP_COLUMNS = 20

FORMAT = 'utf-8'
HEADER = 1024
ACK = "ACK".encode(FORMAT)
OK = "OK".encode(FORMAT)
KO = "KO".encode(FORMAT)
SECONDS = 5
APIPORT = '5000'
CERTIFICATE_FOLDER = "TaxiCertificates/"
#MAP_CONF_FILENAME = "./conf/cityconf.txt"

global x, y, state, active, customerOnBoard, sensorsState, sensorsIDs

global lockState, lockActive, lockCustomerOnBoard, lockSensorsState, lockService

global registered, authenticated, emergency, activeBeforeEmergency

# Token y certificado de la sesión actual  (False si no está autenticado)
global token, certificate, onAuthenticationLoop, onAuthenticationLoopLock, lastAckTime

global mapArray, lockMapArray, lastMapArray, lockLastMapArray

##########  UTILITIES  ##########

# DESCRIPTION: Convierte una posición en formato 000... 399 a coordenadas
# STARTING_VALUES: Posición
# RETURNS: x e y de las coordenadas
# NEEDS: NONE
def decipherPos(pos):
    pos = int(pos)
    y = pos // MAP_COLUMNS
    x = pos % MAP_COLUMNS
    return x, y
#
def calcLocation(posX, posY):
    return posY * MAP_COLUMNS + posX

# DESCRIPTION: Este método recibe un mensaje y devuelve el mensaje codificado en el formato
# token_mensajeCifrado, empleando el token y la clave asociado a la sesión del taxi actual
# STARTING_VALUES: Mensaje a codificar
# RETURNS: mensaje codificado en el formato token_mensajeCifrado
# NEEDS: NONE
def encodeMessage(originalMessage):
    global token
    try:
        certificate = readCertificateOnFile()
        if token:
            f = Fernet(certificate)
            encodedMessage = f.encrypt(originalMessage.encode(FORMAT)).decode(FORMAT)
            finalMessage = f"{token}|{encodedMessage}"
            # print(f"Mensaje codificado con el certificado: {certificate}")
            print(f"[MESSAGE ENCODER] Message '{originalMessage}' encoded correctly")
            return finalMessage.encode(FORMAT)      # Devuelve el mensaje ya en forma de bytes
        return False
    except Exception as e:
        print(f"[MESSAGE ENCODER] THERE HAS BEEN AN ERROR ENCODING THE MESSAGE. {e}")
        return False

# DESCRIPTION: Este método recibe un mensaje codificado en el formato token_mensajeCifrado y, si está dirigido a este taxi,
#  devuelve el mensaje descifrado con el certificado simétrico asociado a la sesión del taxi
# STARTING_VALUES: Mensaje codificado en el formato token_mensajeCifrado
# RETURNS: mensaje descifrado con el certificado simétrico asociado a la sesión del taxi o False si no va dirigido a este taxi
# NEEDS: NONE
def decodeIfForMe(message):
    global token, authenticated
    try:
        stringMessage = message.decode(FORMAT)
        splitMessage = stringMessage.split("|")
        destToken = splitMessage[0]
        encryptedMessage = splitMessage[1].encode(FORMAT)
        # If the message is addressed to this taxi
        if destToken == token:
            certificate = readCertificateOnFile()
            f = Fernet(certificate)
            originalMessage = f.decrypt(encryptedMessage).decode(FORMAT)
            originalMessageSections = originalMessage.split(":")
            if originalMessageSections[0] == destToken:
                print(f"{originalMessageSections[0]} == {destToken}")
                print(f"[MESSAGE DECODER] Message '{originalMessage}' decoded correctly")
                return originalMessageSections[1]
            else:
                print(f"[MESSAGE DECODER] ERROR ON DIRECT MESSAGE DECODING: {originalMessageSections} IS NOT {destToken}")
                authenticated = False
        
        # If the message is addressed to every taxi
        elif destToken == "broadcastMessage":
            broadcastCertificate = readCertificateOnFile(broadcast=True)
            f = Fernet(broadcastCertificate)
            originalMessage = f.decrypt(encryptedMessage).decode(FORMAT)
            originalMessageSections = originalMessage.split(":")
            if originalMessageSections[0] == destToken:
                print(f"{originalMessageSections[0]} == {destToken}")
                return originalMessageSections[1]
            else:
                print(f"[MESSAGE DECODER] ERROR ON BROADCAST MESSAGE DECODING: {originalMessageSections} IS NOT {destToken}")
                authenticated = False

        return False
    except IndexError:
        print("------------------>>>>>>> LIST INDEX OUT OF RANGE")
    except Exception as e:
        print(f"[MESSAGE DECODER] THERE HAS BEEN AN ERROR DECODING THE MESSAGE. {e}")
        authenticated = False
        authenticationLoop()
        return False

# DESCRIPTION: Este método recibe el certificado para el taxi como cadena de caracteres (string) y lo almacena en un fichero con
# el siguiente formato de nombre: taxi_<ID>_session.cert
# STARTING_VALUES: Certificado como cadena de caractere (string)
# RETURNS: NONE
# NEEDS: NONE
def storeCertificateOnFile(certificate, broadcast=False):
    try:
        filePath = f"{CERTIFICATE_FOLDER}taxi_{str(ID)}_session.cert"
        if broadcast:
            filePath = f"{CERTIFICATE_FOLDER}Broadcast.cert"
        f = open(filePath, "w")
        f.write(certificate.decode(FORMAT))
        f.close()
        print(f"[CERTIFICATE HANDLER] New certificate stored successfully")
    except Exception as e:
        print(f"[CERTIFICATE HANDLER] THERE HAS BEEN AN ERROR STORING THE SESSION CERTIFICATE. {e}")
    
def readCertificateOnFile(broadcast=False):
    try:
        filePath = f"{CERTIFICATE_FOLDER}taxi_{str(ID)}_session.cert"
        if broadcast:
            filePath = f"{CERTIFICATE_FOLDER}Broadcast.cert"
       
        f = open(filePath, "r")
        certificate = f.readline().encode(FORMAT)
        f.close()
        print(f"[CERTIFICATE HANDLER] Certificate file read successfully")
        return certificate
    except Exception as e:
        print(f"[CERTIFICATE HANDLER] THERE HAS BEEN AN ERROR STORING THE SESSION CERTIFICATE. {e}")
    


##########  COMMUNICATION WITH CENTRAL  ##########

# DESCRIPTION: Envía a la central el estado del taxi (posición, estado, activo y cliente montado) cada segundo
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: waitForACK()
def sendState():
    global customerOnBoard, x, y, active, state, lockState, lockActive, lockCustomerOnBoard, authenticated
    producer = None
    try:
        # Kafka producer
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        while True:
            if authenticated:
                tempState = ""
                user = ""
                tempActive = ""
                print("hasta aquí 1")
                lockState.acquire()
                if state:
                    tempState = "OK"
                else:
                    tempState = "KO"
                lockState.release()
                print("hasta aquí 2")
                lockActive.acquire()
                if active:
                    tempActive = "Y"
                else:
                    tempActive = "N"
                lockActive.release()
                print("hasta aquí 3")
                lockCustomerOnBoard.acquire()
                if customerOnBoard:
                    user = "Y"
                else: 
                    user = "N"
                lockCustomerOnBoard.release()
                location = calcLocation(x, y)
                print("State not sent yet")
                message = ( ID + "_" + str(location).zfill(3) + "_" + tempState + "_" + tempActive + "_" + user)
                encodedMessage = encodeMessage(message)
                if encodedMessage:
                    producer.send("DE2CentralState", encodedMessage)
                    print("[SEND STATE] Sended: " + message)
                    # Start background process to wait for an answer from Central Control
                    time.sleep(0.5)
                    ackThread = threading.Thread(target=waitForACK)
                    ackThread.daemon=True
                    ackThread.start()
                    time.sleep(0.5)
                else:
                    print("")
    # Manage any exception ocurred
    except KeyboardInterrupt:
        print(f"[SEND STATE] CORRECTLY CLOSED")
        producer.close()
    except Exception as e:
        print(f"[SEND STATE] AN ERROR OCURRED WHILE SENDING MY STATE: {e}")
    finally:
        if producer is not None:
            print(f"[SEND STATE] CLOSING PRODUCER")
            producer.close()
    
# DESCRIPTION: Escucha en el topic de kafka Central2DE:ACK, esperando ACK de los mensajes de estado del taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def waitForACK():
    global authenticated, lastAckTime
    print("ACK function")
    ackListener = None
    try:
        ok = False
        # Kafka consumer
        ackListener = KafkaConsumer("Central2DEACK", bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT), consumer_timeout_ms=SECONDS*1000)
        for message in ackListener:
            decodedMessage = decodeIfForMe(message.value)
            if decodedMessage:
                if ID in decodedMessage:
                    ok = True
                    lastAckTime = time.time()
                    print(f"[SEND STATE] ACK received successfully")
                    break
        if not ok:
            if time.time() - lastAckTime > 10:
                print("[ALARM] LOST CONNECTION WITH CENTRAL CONTROL")
                authenticated = False
                authenticationLoop()
    # Manage any exception ocurred
    except Exception as e:
        print(f"[SEND STATE] AN ERROR OCURRED WHILE SENDING MY STATE: {e}")
    finally:
        if ackListener is not None:
            print(f"[SEND STATE] Closing ACK listener")
            ackListener.close()

# DESCRIPTION: Escucha el topic Central2DEOrder, añadiendo las peticiones de servicio a la lista de tareas
# y ejecutando inmediatamente las órdenes de Stop y Resume
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def receiveInstructions():
    global services, customerOnBoard
    try:
        # Kafka producer and consumer
        receiver = KafkaConsumer("Central2DEOrder", bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        answerManager = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        for message in receiver:
            decodedMessage = decodeIfForMe(message.value)
            if decodedMessage:
                if ID + '_' in decodedMessage:
                    if "STOP" in decodedMessage:
                        # Send ACK message
                        time.sleep(1)
                        encodedAckMessage = encodeMessage(f"ACK_{ID}")                        
                        answerManager.send("DE2CentralACK", encodedAckMessage)
                        stop()

                    elif "RESUME" in decodedMessage:
                        # Send ACK message
                        time.sleep(1)
                        encodedAckMessage = encodeMessage(f"ACK_{ID}")                        
                        answerManager.send("DE2CentralACK", encodedAckMessage)
                        resume()

                    elif "DISMOUNT" in decodedMessage:
                        print("[CUSTOMER MANAGEMENT] Dismounting customer")
                        # Send ACK message
                        time.sleep(1)
                        encodedAckMessage = encodeMessage(f"ACK_{ID}")                        
                        answerManager.send("DE2CentralACK", encodedAckMessage)
                        customerOnBoard = False
                        
                    elif "MOUNT" in decodedMessage:
                        print("[CUSTOMER MANAGEMENT] Mounting customer")
                        # Send ACK message
                        time.sleep(1)
                        encodedAckMessage = encodeMessage(f"ACK_{ID}")                        
                        answerManager.send("DE2CentralACK", encodedAckMessage)
                        customerOnBoard = True
                        
                    elif '_GO_' in decodedMessage:
                        # if not active:

                        # Message format: <TAXIID>_GO_<LOCATION>
                        location = decodedMessage[6:]
                        print(f"[COMMUNICATION] Received message goto {location}")
                        # Send ACK message
                        time.sleep(1)
                        encodedAckMessage = encodeMessage(f"ACK_{ID}")                        
                        answerManager.send("DE2CentralACK", encodedAckMessage)
                        
                        # Start the way to the destination
                        goToThread = threading.Thread(target=goTo, args=(location, ))
                        goToThread.daemon = True
                        goToThread.start()
                        # else:
                        #     print(f"[COMMUNICATION] Asked to go to {location}. But already on another service")
    # Manage any exception ocurred
    except Exception as e:
        print(f"[INSTRUCTION RECEIVER] AN ERROR OCURRED: {e}")
    finally:
        receiver.close()
        answerManager.close()



##########  TAXI MOVEMENTS  ##########

# DESCRIPTION: Modifica las coordenadas del taxi hacia la posición de destino
# STARTING_VALUES: Posición de destino
# RETURNS: NONE
# NEEDS: decipherPos()
def goTo(destPos, prioritary=False):
    global x, y, active, state, lockActive, customerOnBoard, emergency
    try:
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        ackListener = KafkaConsumer("Central2DEServiceACK", bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT), consumer_timeout_ms=SECONDS*1000)
        destX, destY = decipherPos(destPos)

        lockService.acquire()

        lockActive.acquire()
        active = True
        lockActive.release()

        # while ((x != destX or y != destY)):
        while ((x != destX or y != destY) and not (prioritary and not emergency)):
            time.sleep(1.5)
            # if active and state:
            if active and state and prioritary == emergency:
                if x != destX:
                    #Is easier to go through the visible map
                    if abs(destX - x) <= MAP_COLUMNS -  abs(destX - x):
                        #the way is to the right
                        if destX > x:
                            x += 1
                        #the way is to the left
                        else:
                            x -= 1
                    #Is easier to go through the connected border of the map
                    else:
                        #the border is to the right
                        if destX < x:   
                            x += 1
                        #the border is to the left
                        else:
                            x -= 1
                    if x < 0:
                        x = MAP_COLUMNS - 1
                    elif x > MAP_COLUMNS - 1:
                        x = 0

                if y != destY:
                    #Is easier to go through the visible map
                    if abs(destY - y) <= MAP_ROWS -  abs(destY - y):
                        #the way is downwards
                        if destY > y:
                            y += 1
                        #the way is upwards
                        else:
                            y -= 1
                    #Is easier to go through the connected border of the map
                    else:
                        #the border is downwards
                        if destY < y:   
                            y += 1
                        #the border is upwards
                        else:
                            y -= 1
                    if y < 0:
                        y = MAP_ROWS - 1
                    elif y > MAP_ROWS - 1:
                        y = 0
        lockActive.acquire()
        active = False
        lockActive.release()

        lockService.release()

        if not prioritary:
            # If arrived to customer location
            if not customerOnBoard:
                answered = False
                while not answered:
                    #Send signal <TAXI_ID>_ON_LOCATION
                    encodedMessage = encodeMessage(ID+"_ON_LOCATION")
                    producer.send("DE2CentralService", encodedMessage)
                    print(f"[MOTION SERVICE] Arrived to location")
                    for message in ackListener:
                        decodedMessage = decodeIfForMe(message.value)
                        if decodedMessage:
                            if ID in decodedMessage:
                                answered = True
                                print(f"[MOTION SERVICE] Service ended correctly")
            # If arrived to customer destination
            else:
                answered = False
                while not answered:
                    #Send signal <TAXI_ID>_ON_DESTINATION
                    time.sleep(0.5)
                    encodedMessage = encodeMessage(ID+"_ON_DESTINATION")
                    producer.send("DE2CentralService", encodedMessage)
                    print(f"[MOTION SERVICE] Arrived to customer desired destination")
                    for message in ackListener:
                        decodedMessage = decodeIfForMe(message.value)
                        if decodedMessage:
                            if ID in decodedMessage:
                                answered = True
                                print(f"[MOTION SERVICE] Ready to start the service")
        else:
            stop()
            answered = False
            while not answered:
                #Send signal <TAXI_ID>_EMERGENCY_ACTION_SUCCESSFUL
                encodedMessage = encodeMessage(ID+"_EMERGENCY_ACTION_SUCCESSFUL")
                producer.send("DE2CentralService", encodedMessage)
                print(f"[MOTION SERVICE] Arrived to EMERGENCY location")
                for message in ackListener:
                    decodedMessage = decodeIfForMe(message.value)
                    if decodedMessage:
                        if ID in decodedMessage:
                            answered = True
                            print(f"[MOTION SERVICE] Emergency action completed successfully")
    # Manage any exception ocurred
    except Exception as e:
        print(f"[MOTION SERVICE] THERE HAS BEEN AN ERROR ON MOVEMENT CALCULATION OR ARRIVAL INFORMATION: {e}")
    finally:
        producer.close()
        ackListener.close()

# DESCRIPTION: Modifica la variable active para que deje de estar activo el taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def stop():
    global lockActive, active
    print("[TAXI MANAGEMENT] STOPPING TAXI")
    try:
        lockActive.acquire()
        active = False
        lockActive.release()
    except Exception as e:
        print(f"[MOTION SERVICE] THERE HAS BEEN AN ERROR ON TAXI STOPPING: {e}")
            
# DESCRIPTION: Modifica la variable active para que vuelva a estar activo el taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def resume():
    global lockActive, active
    try:
        lockActive.acquire()
        active = True
        lockActive.release()
        lockState.acquire()
        if state:
            print("[TAXI MANAGEMENT] Resuming taxi's way")
        else:
            print("[TAXI MANAGEMENT] Imposible to Resume taxi's way. Taxi is stopped by sensors")
        lockState.release()
    except Exception as e:
        print(f"[MOTION SERVICE] THERE HAS BEEN AN ERROR ON TAXI RESUMING: {e}")
    

def receiveWeatherChanges():
    global active, activeBeforeEmergency
    try:
        receiver = KafkaConsumer("Central2DEWeather", bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
    except Exception as e:
        print(f"[WEATHER RECEIVER] THERE HAS BEEN AN ERROR CREATING THE KAFKA CONSUMER. {e}")

    while True:
        try:
            if authenticated:
                for message in receiver:
                    decodedMessage = decodeIfForMe(message.value)
                    # If weather changed from bad to good
                    if "GOODWEATHER" in decodedMessage:
                        print(f"[WEATHER RECEIVER] Received weather is OK. Resuming services")
                        active = activeBeforeEmergency

                    # If weather changed from good to bad
                    elif "BADWEATHER" in decodedMessage:
                        print(f"[WEATHER RECEIVER] WARNING: Received weather is KO. Returning to Base")
                        activeBeforeEmergency = active
                        emergencyGoToThread = threading.Thread(target=goTo, args=("000", True))
                        emergencyGoToThread.daemon = True
                        emergencyGoToThread.start()

                        

            
                
        # Manage any exception ocurred
        except Exception as e:
            print(f"[MAP RECEIVER] THERE HAS BEEN AN ERROR WHILE RECEIVING MAP STATE INFORMATION. {e}")
        finally:
            receiver.close()


##########  SENSOR MANAGEMENT  ##########

# DESCRIPTION: Inicializa el servicio de recepción de nuevas conexiones de sensores
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: manageSensor()
def startSensorServer():
    global sensorsState
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(ADDR)
        
        print("[SENSOR SERVER] STARTING... Initializing server for communication with sensor")

        server.listen()
        print(f"[SENSOR SERVER] Server for sensors active on {SERVER}:{PORT} ...")
        ACTIVE_SENSORS = 0
        
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=manageSensor, args=(conn, addr))
            thread.daemon = True
            thread.start()
            print(f"[SENSOR SERVER] New sensor connected from {addr}")
            ACTIVE_SENSORS+=1
    except Exception as e:
        print(f'[SENSOR SERVER] THERE HAS BEEN AN ERROR WHILE STARTING THE SENSOR SERVER: {e}')
    finally:
        server.close()

# DESCRIPTION: Mantiene conexión con el sensor y cambia su estado en la lista de estados de sensores (sensorsState)
# STARTING_VALUES: conn [socket for the sensor], addr [IP adress of the sensor], listPosition [position on the list of]
# RETURNS: NONE
# NEEDS: NONE  
def manageSensor(conn, addr):
    global sensorsState, sensorsIDs, lockSensorsState
    conn.settimeout(3)
    connected = True

    newSensorId = conn.recv(HEADER).decode(FORMAT)

    cont = -1
    found = False
    lockSensorsState.acquire()
    for sensorId in sensorsIDs:
        cont = cont + 1
        if sensorId == newSensorId:
            found = True
            break
    listPosition = cont

    if not found:
        sensorsState.append(True)
        sensorsIDs.append(newSensorId)
        listPosition = cont + 1
    lockSensorsState.release()
    last_message_time = time.time()

    while connected:
        try:
            data = conn.recv(HEADER).decode(FORMAT)
            if data:
                lockState.acquire()
                state = data
                lockState.release()
            if state:
                current_time = time.time()
                # If all sensors working correctly and no incidence occurs
                if state == "OK":
                    lockSensorsState.acquire()
                    sensorsState[listPosition] = True
                    lockSensorsState.release()
                # If sensor detects an incidence
                if state == "KO":
                    lockSensorsState.acquire()
                    sensorsState[listPosition] = False
                    lockSensorsState.release()
                last_message_time = current_time
                conn.send(ACK)
            else:
                # Connection closed by client
                print(f"[SENSOR SERVER] Connection with sensor {listPosition + 1} on {addr} closed by client.")
                connected = False
        except socket.timeout:
            # Check if it's been more than 5 seconds since the last message
            if time.time() - last_message_time > 5:
                print(f"[SENSOR SERVER] NO MESSAGE FROM SENSOR {listPosition + 1} ON {addr} FOR MORE THAN 5 SENCONDS")
                lockSensorsState.acquire()
                sensorsState[listPosition] = False
                lockSensorsState.release()
                print(f"[SENSOR SERVER] LOST CONNECTION WITH TAXI SENSOR NUMBER {listPosition + 1} ON {addr}")
            # If not, continue the loop to check again
        except Exception as e:
            print(f"[SENSOR SERVER] ERROR WITH SENSOR ON {addr}: {e}")
            conn.close()
            lockSensorsState.acquire()
            sensorsState[listPosition] = False
            lockSensorsState.release()
            connected = False
    
    print(f"[SENSOR SERVER] Connection with sensor on {addr} closed.")
    conn.close()

# DESCRIPTION: Revisa continuamente que los sensores estén todos correctamente, si uno da error cambia el State del taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def checkSensors():
    global state, sensorsState, lockState, lockSensorsState
    try:
        while True:
            ok = True
            lockSensorsState.acquire()
            for sensor in sensorsState:
                if not sensor:
                    ok = False
                    break
            lockSensorsState.release()
            lockState.acquire()
            state = ok
            lockState.release()
    except Exception as e:
        print(f"[SENSOR MANAGEMENT]: THERE HAS BEEN AN ERROR WHILE CHECKING THE STATE OF THE SENSORS {e}")

##########  TAXI AUTHENTICATION  ##########

def authenticationLoop():
    global authenticated, onAuthenticationLoop, onAuthenticationLoopLock
    onAuthenticationLoopLock.acquire()
    if not onAuthenticationLoop:
        onAuthenticationLoop = True        
        while not authenticated:
            authenticate(reauth=True)
            time.sleep(3)
        onAuthenticationLoop = False
    onAuthenticationLoopLock.release()



# Connection through Sockets
# DESCRIPTION: Sends a message to the Central Control with the taxi ID and waits for it to answer with OK or KO
# STARTING_VALUES: NONE
# RETURNS: True [si se ha realizado correctamente]; False si ha habido algún problema
# NEEDS: NONE
def authenticate(reauth=False):
    global token, authenticated, lastAckTime
    
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    client = context.wrap_socket(conn)
    
    # Stablish connection and send message 
    try:
        print(f"[AUTHENTICATION PROCESS]: Stablishing connection with Central Control Server")
        print(EC_CENTRAL_ADDR)
        client.connect((EC_CENTRAL_ADDR[0], int(EC_CENTRAL_ADDR[1])))
        print(f"[AUTHENTICATION PROCESS]: Sending Taxi ID: {ID}")
        message = str(ID) + "_" + str(PASSWD)
        if reauth:
            message = message + "_reauth"
        else:
            message = message + "_auth"
        client.send(message.encode(FORMAT))
    except ConnectionError:
        print(f"[AUTHENTICATION PROCESS]: UNABLE TO FINDE CENTRAL CONTROL SERVER ON {EC_CENTRAL_ADDR}")
        client.close()
    except Exception as e:
        print(f"[AUTHENTICATION PROCESS]: ERROR ON {EC_CENTRAL_ADDR}. {e}")

    try:
        while True:
            receivedMessage = client.recv(HEADER).decode(FORMAT)
            print(f"--------------> BORRAR: MENSAJE RECIBIDO = {receivedMessage}")
            splitMessage = receivedMessage.split("|")
            answer = splitMessage[0]#.decode(FORMAT)
            print("Answer = "+answer)
            if answer:
                lastAckTime = time.time()
                if answer == "OK":
                    print(f"[AUTHENTICATION PROCESS]: Authenticated sucessfully")
                    token = splitMessage[1]#.decode(FORMAT)
                    storeCertificateOnFile(splitMessage[2].encode(FORMAT))
                    broadcastCertificate = splitMessage[3].encode(FORMAT)
                    storeCertificateOnFile(broadcastCertificate, broadcast=True)
                    client.close()
                    authenticated = True
                    return True
                      
                elif answer == "KO":
                    print(f"[AUTHENTICATION PROCESS]: Authentication failed")
                    client.close()  
                    return False
                else:
                    print(f"[AUTHENTICATION PROCESS]: ERROR: MESSAGE NOT UNDERSTOOD")
                    client.close() 
                    return False
    except ConnectionError:
        print(f"[AUTHENTICATION PROCESS] ERROR: UNABLE TO FIND CENTRAL CONTROL SERVER ON {EC_CENTRAL_ADDR}")
    except Exception as e:
        print(f"[AUTHENTICATION PROCESS] SOMETHING WENT WRONG: {e}")
    finally: 
        client.close()

##########  REGISTRY METHODS  ##########
# DESCRIPTION: Da de baja al taxi, pasandole la id y password por parámetro
# STARTING_VALUES: id del taxi, y la contraseña
# RETURNS: -1, si ha habido un error, 0 si no se ha encontrado un taxi con los mismos parámetros, 1 si se ha podido borrar
# NEEDS: NONE
def leave(id, password):
    global registered, authenticated
    url = 'https://' + REGISTRYIP + ':' + APIPORT + '/deleteTaxi'
    payload = {'id': str(id), 'password': str(password)}
    response = requests.post(url, json=payload, verify=False)
    response = response.json()
    if response["response"] == 'OK':
        registered = False
        return 1
    elif response["response"] == 'KO':
        return 0
    elif response["response"] == 'ERR':
        return -1
    authenticated = False

# DESCRIPTION: Da de alta al taxi, pasandole la id y password por parámetro
# STARTING_VALUES: id del taxi, y la contraseña
# RETURNS: -1, si ha habido un error, 0 si ya hay un taxi con esa id, 1 si se ha podido registrar correctamente
# NEEDS: NONE
def register(id, password):
    global registered
    url = 'https://' + REGISTRYIP + ':' + APIPORT + '/addTaxi'
    payload = {'id': str(id), 'password': str(password)}
    print("eyeye1")
    response = requests.post(url, json=payload, verify=False)
    print("eyeye2")
    response = response.json()
    print("eyeye3")
    if response["response"] == 'OK':
        registered = True
        return 1
    elif response["response"] == 'KO':
        return 0
    elif response["response"] == 'ERR':
        return -1


############ GRAPHICAL USER INTERFACE ############ 

# DESCRIPTION: Recibe la cadena de caracteres con la información del estado del mapa y actualiza este en memoria y en la GUI
# STARTING_VALUES: El array de botones de la GUI
# RETURNS: NONE
# NEEDS: translateMapMessage
def receiveMapState(mapButtons):
    global lastAckTime
    try:
        receiver = KafkaConsumer("Central2DEMap", bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
    except Exception as e:
        print(f"[MAP RECEIVER] THERE HAS BEEN AN ERROR CREATING THE KAFKA CONSUMER. {e}")

    while True:
        try:
            if authenticated:
                # Kafka producer and consumer
                for message in receiver:
                    lastAckTime = time.time()
                    decodedMessage = decodeIfForMe(message.value)
                    if decodedMessage:
                        translateMapMessage(decodedMessage)
                        updateMap(mapButtons)
            
                
        # Manage any exception ocurred
        except Exception as e:
            print(f"[MAP RECEIVER] THERE HAS BEEN AN ERROR WHILE RECEIVING MAP STATE INFORMATION. {e}")
            receiver.close()

# DESCRIPTION: Convierte la cadena de caracteres recibida en un array y actualiza el estado del mapa
# STARTING_VALUES: La cadena de caracteres con la información del mapa
# RETURNS: NONE
# NEEDS: NONE
def translateMapMessage(mapMessage):
    global lockMapArray, mapArray, lastMapArray, lockLastMapArray
    # time.sleep(0.8)
    lockLastMapArray.acquire()
    lockMapArray.acquire()
    # Deep copy of the map array
    lastMapArray = copy.deepcopy(mapArray)
    mapArray = str(mapMessage).split(",")
    lockMapArray.release()
    lockLastMapArray.release()

# DESCRIPTION: Imprime por pantalla la casilla seleccionada
# STARTING_VALUES: x e y de la casilla
# RETURNS: NONE
# NEEDS: NONE
def onMapClick(x, y):
        print(f"Clicked on position: ({x}, {y})")

# DESCRIPTION: Actualiza el mapa en la pantalla
# STARTING_VALUES: mapa, en forma de matriz de botones
# RETURNS: NONE
# NEEDS: NONE
def updateMap(mapButtons):
    global lockMapArray, mapArray, lastMapArray, lockLastMapArray
    try:
        # Changing map cells' state
        for i in range(MAP_COLUMNS):
            for j in range(MAP_ROWS):
                lockMapArray.acquire()
                lockLastMapArray.acquire()
                if mapArray[i* MAP_COLUMNS + j] != lastMapArray[i* MAP_COLUMNS + j]:
                    lockMapArray.release()
                    lockLastMapArray.release()
                    print("wow")
                    color = "white"
                    infoText = ""
                    lockMapArray.acquire()
                    item = str(mapArray[i* MAP_COLUMNS + j])
                    lockMapArray.release()
                    
                    if item != "#":
                        infoText = item
                        # Item is a location or a client
                        if item.isalpha():
                            # It is a location
                            if item.isupper():
                                color = "cyan"
                            # It is a location
                            elif item.islower():
                                color = "yellow"
                        # Item is a moving taxi
                        elif item.isnumeric() or item.isalnum():
                            color = "green"
                        # Item is a broken or stopped taxi
                        if "!" in item or "-" in item:
                            color = "red"   
                            if "-" in item:
                                item.replace("-", "")
                    mapButtons[i][j].configure(bg = color, text = infoText)
                else:
                    lockMapArray.release()
                    lockLastMapArray.release()
        print(f"[GUI MAP CREATOR]: Updated map.")                    
                    
        # root.after(500, lambda: updateMap(mapButtons, root))
    except Exception as e:
        print(f"[GUI MAP UPDATER]: THERE HAS BEEN AN ERROR WHILE UPDATING THE MAP. {e}")
    
# DESCRIPTION: Actualiza la información del taxi
# STARTING_VALUES: El objeto etiqueta de la GUI
# RETURNS: NONE
# NEEDS: NONE
def updateInfoGUI(mapButtons, root):
    global authenticated, registered
     # Información sobre estado del taxi
    stateInfo = "Estado: "

    if authenticated:
        stateInfo = stateInfo + "AUTENTICADO"
    elif registered:
        stateInfo = stateInfo + "REGISTRADO"
    else:
        stateInfo = stateInfo + "SIN REGISTRAR"

    infoLabel.config(text=stateInfo)
    root.after(1000, lambda: updateInfoGUI(infoLabel, root))


# DESCRIPTION: Almacena en selectedPos la posición del mapa seleccionada
# STARTING_VALUES: Frame principal de la interfaz gráfica
# RETURNS: el mapa, en forma de matriz de botones
# NEEDS: NONE
def createGUI(mainFrame):
    global registered, authenticated
    try:
        # MENÚ PARA EL CONTROL DE REGISTRO Y AUTENTICACIÓN
        # Marco de información y control sobre el taxi
        leftFrame = ttk.Frame(mainFrame)
        leftFrame.grid(row=0, column=0, padx=30, pady=10)

        menuFrame = ttk.LabelFrame(leftFrame, text="MENÚ")
        menuFrame.grid(row=0, column=0, padx=5, pady=5, sticky=(tk.W, tk.E))

        # Marco de información del taxi
        infoFrame = ttk.LabelFrame(menuFrame, text="TAXI "+ str(ID) +" INFO:")
        infoFrame.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        # Información sobre estado del taxi
        stateInfo = "Estado: "

        if authenticated:
            stateInfo = stateInfo + "AUTENTICADO"
        elif registered:
            stateInfo = stateInfo + "REGISTRADO"
        else:
            stateInfo = stateInfo + "SIN REGISTRAR"

        infoLabel = ttk.Label(infoFrame, text=stateInfo)
        infoLabel.pack(padx=5, pady=5)

        optionsFrame = ttk.LabelFrame(menuFrame, text="OPCIONES")
        optionsFrame.grid(row=1, column=0, sticky=(tk.W, tk.E))

        # Opciones del menú
        registerButton = ttk.Button(optionsFrame, text="REGISTRAR", command=lambda: register(ID, PASSWD))
        registerButton.pack(side=tk.BOTTOM, padx=5)

        authenticateButton = ttk.Button(optionsFrame, text="AUTENTICAR", command=lambda: authenticate())
        authenticateButton.pack(side=tk.BOTTOM, padx=5)

        # MAPA

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
        print(f"[GUI CREATOR]: finished GUI start")
        return infoLabel, mapButtons
    except Exception as e:
        print(f"[GUI CREATOR]: THERE HAS BEEN AN ERROR WHILE CREATING THE GUI. {e}")




########## STARTING POINT OF MAIN APPLICATION ##########

if (len(sys.argv) == 9):
    # Argument management
    EC_CENTRAL_IP = sys.argv[1]
    EC_CENTRAL_PORT = int(sys.argv[2])
    BROKER_IP = sys.argv[3]
    BROKER_PORT = int(sys.argv[4])
    ID = str(sys.argv[5]).zfill(2)
    PASSWD = str(sys.argv[6])
    SERVER = sys.argv[7]
    PORT = int(sys.argv[8])
    # Preparing data for future uses
    ADDR = (SERVER, PORT)
    EC_CENTRAL_ADDR = (EC_CENTRAL_IP, EC_CENTRAL_PORT)
    BROKER_ADDR = (BROKER_IP, BROKER_PORT)
    REGISTRYIP = sys.argv[1]
    

    x = y = 0
    token = False
    certificate = False
    active = False
    state = True
    customerOnBoard = False
    registered = False
    authenticated = False
    emergency = False
    activeBeforeEmergency = True
    onAuthenticationLoop = False
    lastAckTime = False
    sensorsState = []
    sensorsIDs = []
    mapArray = []
    lastMapArray = []
    lockState = threading.Lock()
    lockActive = threading.Lock()
    lockMapArray = threading.Lock()
    lockCustomerOnBoard = threading.Lock()
    lockSensorsState = threading.Lock()
    lockLastMapArray = threading.Lock()
    lockService = threading.Lock()
    onAuthenticationLoopLock = threading.Lock()


    for i in range(MAP_COLUMNS * MAP_ROWS):
        mapArray.append("#")
    

    try:
        # Graphical User Interface
        root = tk.Tk()
        root.title("EasyCab Digital Engine Info Panel")
        mainFrame = ttk.Frame(root, padding="10")
        mainFrame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        # Create map of size 20x20
        infoLabel, mapButtons = createGUI(mainFrame)
        root.after(8000, lambda: updateInfoGUI(infoLabel, root))

        ## Creating a thread for Sensor Server ##
        sensorThread = threading.Thread(target=startSensorServer)
        sensorThread.daemon = True
        sensorThread.start()
        ## Creating a thread for Checking the state of all sensors ##
        checkState = threading.Thread(target=checkSensors)
        checkState.daemon = True
        checkState.start()
        ## Creating a thread for sending periodically the state of the taxi ##
        sendStateThread = threading.Thread(target=sendState)
        sendStateThread.daemon = True
        sendStateThread.start()

        ## Creating a thread for receiving orders from Central Control ##
        receiveInstructionsThread = threading.Thread(target=receiveInstructions)
        receiveInstructionsThread.daemon = True
        receiveInstructionsThread.start()
        
        ## Creating a thread for receiving orders from Central Control ##
        receiveMapStateThread = threading.Thread(target=receiveMapState, args=(mapButtons, ))
        receiveMapStateThread.daemon = True
        receiveMapStateThread.start()

            
        root.mainloop()
        
        # Printing exit message
        print("[DIGITAL ENGINE] Execution finished. Exiting application")
    
    except KeyboardInterrupt:
        print(f"[DIGITAL ENGINE] Application shutdown due to human interaction")
    except Exception as e:
        print(f"[DIGITAL ENGINE] THERE HAS BEEN AN ERROR. {e}")
    finally:
        print(f'EXITING DIGITAL ENGINE APPLICATION')

else:
    print("SORRY, INCORRECT PARAMETER USE. \nUSAGE: <EC_CENTRAL IP> <EC_CENTRAL PORT> <BROKER IP> <BROKER PORT> <TAXI ID> <PASSWORD> <LISTENING_IP_SENSORS> <LISTENING_PORT_SENSORS>")