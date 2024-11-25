import sys
import threading
import time
import socket
from kafka import KafkaProducer, KafkaConsumer
import tkinter as tk
from tkinter import ttk
from encodings import idna

MAP_ROWS = 20
MAP_COLUMNS = 20
SERVER = "172.21.242.83"
FORMAT = 'utf-8'
HEADER = 1024
ACK = "ACK".encode(FORMAT)
OK = "OK".encode(FORMAT)
KO = "KO".encode(FORMAT)
SECONDS = 10
#MAP_CONF_FILENAME = "./conf/cityconf.txt"

global x, y, state, active, customerOnBoard, sensorsState

global lockState, lockActive

global mapArray, lockMapArray

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

##########  COMMUNICATION WITH CENTRAL  ##########

# DESCRIPTION: Envía a la central el estado del taxi (posición, estado, activo y cliente montado) cada segundo
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: waitForACK()
def sendState():
    global customerOnBoard, x, y, active, state
    try:
        # Kafka producer
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        while True:
            tempState = ""
            user = ""
            tempActive = ""
            if state:
                tempState = "OK"
            else:
                tempState = "KO"
            if active:
                tempActive = "Y"
            else:
                tempActive = "N"
            if customerOnBoard:
                user = "Y"
            else: 
                user = "N"
            location = calcLocation(x, y)
            producer.send("DE2CentralState", ( ID + "_" + str(location).zfill(3) + "_" + tempState + "_" + tempActive + "_" + user).encode(FORMAT))
            print("[SEND STATE]" + ID + "_" + str(location).zfill(3) + "_" + tempState + "_" + tempActive + "_" + user)
            # Start background process to wait for an answer from Central Control
            time.sleep(0.5)
            ackThread = threading.Thread(target=waitForACK)
            ackThread.start()
          
    # Manage any exception ocurred
    except KeyboardInterrupt:
        print(f"[SEND STATE] CORRECTLY CLOSED")
        producer.close()
    except Exception as e:
        print(f"[SEND STATE] AN ERROR OCURRED WHILE SENDING MY STATE: {e}")
    finally:
        producer.close()
    
# DESCRIPTION: Escucha en el topic de kafka Central2DE:ACK, esperando ACK de los mensajes de estado del taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def waitForACK():
    try:
        ok = False
        # Kafka consumer
        ackListener = KafkaConsumer("Central2DEACK", bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT), consumer_timeout_ms=SECONDS*1000)
        for message in ackListener:
            decodedMessage = message.value.decode(FORMAT)
            if ID in decodedMessage:
                ok = True
                break
        if not ok:
            print("[ALARM] LOST CONNECTION WITH CENTRAL CONTROL")
            # connected = False
            #
            # TODO: Decide what should be done in this case
            #
    # Manage any exception ocurred
    except Exception as e:
        print(f"[SEND STATE] AN ERROR OCURRED WHILE SENDING MY STATE: {e}")
    finally:
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
            decodedMessage = message.value.decode(FORMAT)
            if ID + '_' in decodedMessage:
                if "STOP" in decodedMessage:
                    # Send ACK message
                    time.sleep(1)
                    answerManager.send("DE2CentralACK", ("ACK_" + ID).encode(FORMAT))
                    stop()

                elif "RESUME" in decodedMessage:
                    # Send ACK message
                    time.sleep(1)
                    answerManager.send("DE2CentralACK", ("ACK_" + ID).encode(FORMAT))
                    resume()

                elif "DISMOUNT" in decodedMessage:
                    print("[CUSTOMER MANAGEMENT] Dismounting customer")
                    # Send ACK message
                    time.sleep(1)
                    answerManager.send("DE2CentralACK", ("ACK_" + ID).encode(FORMAT))
                    customerOnBoard = False
                    
                elif "MOUNT" in decodedMessage:
                    print("[CUSTOMER MANAGEMENT] Mounting customer")
                    # Send ACK message
                    time.sleep(1)
                    answerManager.send("DE2CentralACK", ("ACK_" + ID).encode(FORMAT))
                    customerOnBoard = True
                    
                elif '_GO_' in decodedMessage:
                    if not active:
                        # Message format: <TAXIID>_GO_<LOCATION>
                        location = decodedMessage[6:]
                        print(f"[COMMUNICATION] Received message goto {location}")
                        # Send ACK message
                        time.sleep(1)
                        answerManager.send("DE2CentralACK", ("ACK_" + ID).encode(FORMAT))
                        
                        # Start the way to the destination
                        goToThread = threading.Thread(target=goTo, args=(location, ))
                        goToThread.daemon = True
                        goToThread.start()
                    else:
                        print(f"[COMMUNICATION] Asked to go to {location}. But already on another service")
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
def goTo(destPos):
    global x, y, active, state, lockActive, customerOnBoard
    try:
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
        ackListener = KafkaConsumer("Central2DEServiceACK", bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT), consumer_timeout_ms=SECONDS*1000)
        destX, destY = decipherPos(destPos)
        lockActive.acquire()
        active = True
        lockActive.release()

        while (x != destX or y != destY):
            time.sleep(1.5)
            if active and state:
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

        # If arrived to customer location
        if not customerOnBoard:
            answered = False
            while not answered:
                #Send signal <TAXI_ID>_ON_LOCATION
                producer.send("DE2CentralService", (ID+"_ON_LOCATION").encode(FORMAT))
                print(f"[MOTION SERVICE] Arrived to location")
                for message in ackListener:
                    decodedMessage = message.value.decode(FORMAT)
                    if ID in decodedMessage:
                        answered = True
                        print(f"[MOTION SERVICE] Service ended correctly")
        # If arrived to customer destination
        else:
            answered = False
            while not answered:
                #Send signal <TAXI_ID>_ON_DESTINATION
                time.sleep(0.5)
                producer.send("DE2CentralService", (ID+"_ON_DESTINATION").encode(FORMAT))
                print(f"[MOTION SERVICE] Arrived to customer desired destination")
                for message in ackListener:
                    decodedMessage = message.value.decode(FORMAT)
                    if ID in decodedMessage:
                        answered = True
                        print(f"[MOTION SERVICE] Ready to start the service")
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
        if state:
            print("[TAXI MANAGEMENT] Resuming taxi's way")
        else:
            print("[TAXI MANAGEMENT] Imposible to Resume taxi's way. Taxi is stopped by sensors")
    except Exception as e:
        print(f"[MOTION SERVICE] THERE HAS BEEN AN ERROR ON TAXI RESUMING: {e}")
    
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
            thread = threading.Thread(target=manageSensor, args=(conn, addr, ACTIVE_SENSORS))
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
def manageSensor(conn, addr, listPosition):
    global sensorsState
    conn.settimeout(1)
    sensorsState.append(True)
    connected = True
    last_message_time = time.time()
    
    while connected:
        try:
            state = conn.recv(HEADER).decode(FORMAT)
            if state:
                current_time = time.time()
                # If all sensors working correctly and no incidence occurs
                if state == "OK":
                    sensorsState[listPosition] = True
                # If sensor detects an incidence
                if state == "KO":
                    sensorsState[listPosition] = False
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
                sensorsState[listPosition] = False
                print(f"[SENSOR SERVER] LOST CONNECTION WITH TAXI SENSOR NUMBER {listPosition + 1} ON {addr}")
            # If not, continue the loop to check again
        except Exception as e:
            print(f"[SENSOR SERVER] ERROR WITH SENSOR ON {addr}: {e}")
            conn.close()
            sensorsState[listPosition] = False
            connected = False
    
    print(f"[SENSOR SERVER] Connection with sensor on {addr} closed.")
    conn.close()

# DESCRIPTION: Revisa continuamente que los sensores estén todos correctamente, si uno da error cambia el State del taxi
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def checkSensors():
    global state, sensorsState
    try:
        while True:
            ok = True
            for sensor in sensorsState:
                if not sensor:
                    ok = False
                    break
            state = ok
    except Exception as e:
        print(f"[SENSOR MANAGEMENT]: THERE HAS BEEN AN ERROR WHILE CHECKING THE STATE OF THE SENSORS")

##########  TAXI AUTHENTICATION  ##########

# Connection through Sockets
# DESCRIPTION: Sends a message to the Central Control with the taxi ID and waits for it to answer with OK or KO
# STARTING_VALUES: EC_CENTRAL_ADDR [IP:Puerto de la central]
# RETURNS: True [si se ha realizado correctamente]; False si ha habido algún problema
# NEEDS: NONE
def authenticate(EC_CENTRAL_ADDR):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Stablish connection and send message 
    try:
        print(f"[AUTHENTICATION PROCESS]: Stablishing connection with Central Control Server")
        print(EC_CENTRAL_ADDR)
        client.connect((EC_CENTRAL_ADDR[0], int(EC_CENTRAL_ADDR[1])))
        print(f"[AUTHENTICATION PROCESS]: Sending Taxi ID: {ID}")
        client.send(ID.encode(FORMAT))
    except ConnectionError:
        print(f"[AUTHENTICATION PROCESS]: Unable to find CENTRAL CONTROL SERVER on {EC_CENTRAL_ADDR}")
        client.close()
    except Exception as e:
        print(f"[AUTHENTICATION PROCESS]: Error on {EC_CENTRAL_ADDR}. {e}")

    try:
        while True:
            answer = client.recv(HEADER).decode(FORMAT)
            if answer:
                if answer == "OK":
                    print(f"[AUTHENTICATION PROCESS]: Authenticated sucessfully")
                    client.close()
                    return True
                      
                elif answer == "KO":
                    print(f"[AUTHENTICATION PROCESS]: Authenticated failed")
                    client.close()  
                    return False
                else:
                    print(f"[AUTHENTICATION PROCESS]: Error 'message not understood'")
                    client.close() 
                    return False
    except ConnectionError:
        print(f"[AUTHENTICATION PROCESS] ERROR: UNABLE TO FIND CENTRAL CONTROL SERVER ON {EC_CENTRAL_ADDR}")
    except Exception as e:
        print(f"[AUTHENTICATION PROCESS] SOMETHING WENT WRONG: {e}")
    finally: 
        client.close()


############ GRAPHICAL USER INTERFACE ############ 

def receiveMapState():
    while True:
        try:
            # Kafka producer and consumer
            receiver = KafkaConsumer("Central2DEMap", bootstrap_servers=str(BROKER_IP) + ':' + str(BROKER_PORT))
            for message in receiver:
                decodedMessage = message.value.decode(FORMAT)
                translateMapMessage(decodedMessage)
            
                
        # Manage any exception ocurred
        except Exception as e:
            print(f"[MAP RECEIVER] THERE HAS BEEN AN ERROR WHILE RECEIVING MAP STATE INFORMATION: {e}")
        finally:
            receiver.close()

def translateMapMessage(mapMessage):
    global lockMapArray, mapArray
    lockMapArray.acquire()
    mapArray = str(mapMessage).split(",")
    lockMapArray.release()



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
def updateMap(mapButtons, root):
    global lockMapArray, mapArray
    try:
        # Changing map cells' state
        for i in range(MAP_COLUMNS):
            for j in range(MAP_ROWS):
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
        print(f"[GUI MAP CREATOR]: Updated map.")                    
                    
        root.after(1000, lambda: updateMap(mapButtons, root))
    except Exception as e:
        print(f"[GUI MAP UPDATER]: THERE HAS BEEN AN ERROR WHILE UPDATING THE MAP. {e}")
    


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




########## STARTING POINT OF MAIN APPLICATION ##########

if (len(sys.argv) == 7):
    # Argument management
    EC_CENTRAL_IP = sys.argv[1]
    EC_CENTRAL_PORT = int(sys.argv[2])
    BROKER_IP = sys.argv[3]
    BROKER_PORT = int(sys.argv[4])
    ID = str(sys.argv[5]).zfill(2)
    PORT = int(sys.argv[6])
    # Preparing data for future uses
    ADDR = (SERVER, PORT)
    EC_CENTRAL_ADDR = (EC_CENTRAL_IP, EC_CENTRAL_PORT)
    BROKER_ADDR = (BROKER_IP, BROKER_PORT)

    x = y = 0
    active = False
    state = True
    customerOnBoard = False
    sensorsState = []
    mapArray = []
    lockState = threading.Lock()
    lockActive = threading.Lock()
    lockMapArray = threading.Lock()

    for i in range(MAP_COLUMNS * MAP_ROWS):
        mapArray.append("#")
    

    try:
        # Graphical User Interface
        root = tk.Tk()
        root.title("EasyCab Digital Engine Info Panel")
        mainFrame = ttk.Frame(root, padding="10")
        mainFrame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        # Create map of size 20x20
        mapButtons = createMap(mainFrame)
        


        if authenticate(EC_CENTRAL_ADDR):
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
            receiveMapStateThread = threading.Thread(target=receiveMapState)
            receiveMapStateThread.daemon = True
            receiveMapStateThread.start()

            root.after(1000, lambda: updateMap(mapButtons, root))
            root.mainloop()
            # while True:
            #     pass
        
        # Printing exit message
        print("[DIGITAL ENGINE] Execution finished. Exiting application")
    
    except KeyboardInterrupt:
        print(f"[DIGITAL ENGINE] Application shutdown due to human interaction")
    except Exception as e:
        print(f"[DIGITAL ENGINE] THERE HAS BEEN AN ERROR. {e}")
    finally:
        print(f'EXITING DIGITAL ENGINE APPLICATION')

else:
    print("SORRY, INCORRECT PARAMETER USE. \nUSAGE: <EC_CENTRAL IP> <EC_CENTRAL PORT> <BROKER IP> <BROKER PORT> <TAXI ID> <LISTENING_PORT_SENSORS>")