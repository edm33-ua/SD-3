from kafka import KafkaConsumer, KafkaProducer
import sys
import json
import time
import tkinter as tk
from tkinter import ttk
import threading

FORMAT = 'utf-8'
SECONDS = 5

global currentPosition, state

##########  JSON FILE MANAGEMENT  ##########

# DESCRIPTION: Lee la información contenida en el fichero EC_Requests.json y la devuelve en forma de string
# STARTING_VALUES: NONE
# RETURNS: JSON string with all the data
# NEEDS: NONE
def readJson():
    try:
        print(f"[JSON READING] Reading .json requests file")
        # Open and read the JSON file
        f = open('EC_Requests.json')
        data = json.load(f)
        f.close()
        return data
    except Exception as e:
         f.close()
         print(f"[JSON READING] THERE HAS BEEN AN ERROR WHILE READING JSON FILE: {e}")

##########  COMMUNICATION WITH CENTRAL  ##########

# DESCRIPTION: Permanece a la escucha de información enviada por la central a su ClientID.
# Si la información es MOUNT, devuelve True, si es FINISH, devuelve False
# STARTING_VALUES: NONE
# RETURNS: True si la información es MOUNT; False si es FINISH
# NEEDS: NONE
def mountOrUnmount():
    global currentPosition, state
    producer = None
    consumer = None
    try:
        # We create the kafka producer and the consumer
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT))
        consumer = KafkaConsumer('Central2CustomerInformation', bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT))
         # any important information regarding our client
        for message in consumer:
            # We get a decoded version of the message value from kafka
            decodedMessage = message.value.decode(FORMAT)
            # If our ID is in the message
            if CLIENT_ID in decodedMessage:
                # The message is 
                if "MOUNT" in decodedMessage:
                    print(f'[SERVICE MANAGEMENT] The taxi has arrived to your location. Please mount on the vehicle')
                    time.sleep(1)
                    producer.send('Customer2CentralACK', (CLIENT_ID + '_ACK').encode(FORMAT))
                    state = "Montado"
                    return True
                elif 'FINISHED' in decodedMessage:
                    print(f'[SERVICE MANAGEMENT] Your taxi has arrived to the destination. Please dismount from the vehicle')
                    # Received message format: <CLIENTID>_FINISHED_<LOCATION>
                    # Example: a_FINISHED_129
                    currentPosition = decodedMessage[11:]
                    time.sleep(1)
                    producer.send('Customer2CentralACK', (CLIENT_ID + '_ACK').encode(FORMAT))
                    state = "Finalizado"
                    return False
                elif 'BROKE' in decodedMessage:
                    print(f'[SERVICE MANAGEMENT] Your taxi has broken down. We\'re sorry, but here ends your journey')
                    # Received message format: <CLIENTID>_BROKE_<LOCATION>
                    # Example: a_BROKE_129
                    currentPosition = decodedMessage[11:]
                    state = "Taxi roto"
                    return None
                else:
                    print(f'[SERVICE MANAGEMENT] ERROR: UNEXPECTED MESSAGE FROM CENTRAL CONTROL')
        
    # We catch and print any exceptions in case they happen
    except Exception as e:
            print(f'[SERVICE MANAGEMENT] THERE HAS BEEN AN ERROR WHEN LISTENING FOR CENTRAL MESSAGES: {e}')
    finally:
        # We close both the consumer and producer in case there is an error
        if consumer != None:
            consumer.close()
        if producer != None:
            producer.close()
     

# DESCRIPTION: Este método pregunta a la Central si hay algún taxi disponible
# y devuelve True o False según la respuesta de la central
# STARTING_VALUES: NONE
# RETURNS: True si lo hay y False, si no
# NEEDS: NONE
def askForTaxi(destination):
    global state
    producer = None
    consumer = None
    try:
        # We create the kafka producer and the consumer
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT))
        consumer = KafkaConsumer('Central2CustomerInformation', bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT), consumer_timeout_ms=SECONDS*1000)
        
        answered = False
        while not answered:
            # We ask for a taxi through the kafka producer
            print(f'[PETITION MANAGEMENT] Asking Central Control for a service to {destination}')
            producer.send('Customer2CentralPetitions', (CLIENT_ID + '_ASK_' + currentPosition.zfill(3) + "_" + destination + "_" + SERVER_IP).encode(FORMAT))
            state = "Taxi pedido"
            print("Petición enviada")
            # For each message that has arrived since we've created the kafka consumer, we will check if it has
            # any important information regarding our client
            for message in consumer:
                # We get a decoded version of the message value from kafka
                decodedMessage = message.value.decode(FORMAT)
                print(decodedMessage)
                # If our ID is in the message
                if CLIENT_ID in decodedMessage:
                    answered = True
                    # then search if the substring informing about the taxi situation is in there
                    # If it's in there, we return the ID of the taxi,
                    if "OK" in decodedMessage:
                        print(f'[PETITION MANAGEMENT] Service to {destination} accepted by Central Control')
                        state = "Petición aceptada. Esperando taxi"
                        time.sleep(1)
                        producer.send('Customer2CentralACK', (CLIENT_ID + '_ACK').encode(FORMAT))
                        # We close both the consumer and producer in case there is an error
                        # consumer.close()
                        # producer.close()
                        return True
                    # but if there aren't any, we return NaN, which will log off the client
                    elif 'KO' in decodedMessage:
                        print(f'[PETITION MANAGEMENT] Service to {destination} rejected by Central Control')
                        state = "Petición rechazada. Esperando para nueva solicitud"
                        producer.send('Customer2CentralACK', (CLIENT_ID + '_ACK').encode(FORMAT))
                        # We close both the consumer and producer in case there is an error
                        # consumer.close()
                        # producer.close()
                        return False
            print(f'[PETITION MANAGEMENT] THERE HAS BEEN NO RESPONSE FROM CENTRAL CONTROL')
            print(f'[PETITION MANAGEMENT] SENDING PETITION AGAIN')
    # We catch and print any exceptions in case they happen
    except Exception as e:
        print(f'[PETITION MANAGEMENT] THERE HAS BEEN AN ERROR WHILE ASKING FOR A TAXI: {e}, PLEASE TRY AGAIN LATER')
    finally:
        # We close both the consumer and producer in case there is an error
        if consumer != None:
            consumer.close()
        if producer != None:
            producer.close()

def programLoop():
    for petition in data["Requests"]:
                locationID = petition["Id"]
                print(f"Going to: {locationID}")
                if askForTaxi(locationID):
                    resultado1 = mountOrUnmount()
                    if resultado1 is not None:
                        if resultado1:
                            resultado2 = mountOrUnmount()
                            if resultado2 is not None:
                                if resultado2:
                                    print(f"[CUSTOMER APPLICATION] ERROR: IMPOSSIBLE TO MOUNT. ALREDY MOUNTED.")
                        else:
                            print(f"[CUSTOMER APPLICATION] ERROR: IMPOSSIBLE TO DISMOUNT. IF NOT MOUNTED YET.")
                time.sleep(4)
 

# DESCRIPTION: Crea los elementos de la interfaz gráfica
# STARTING_VALUES: el marco principal de la aplicación
# RETURNS: Las etiquetas (Label) de la interfaz que muestran el estado y la posición del usuario
# NEEDS: NONE
def designInterface(mainFrame):
    global currentPosition, state
    textFrame = ttk.Frame(mainFrame)
    textFrame.grid(row=1, column=0, padx=10, pady=10)

    mainLabel = ttk.LabelFrame(textFrame, text="BIENVENIDO, USUARIO "+ str(CLIENT_ID))
   
    infoLabel = ttk.Label(mainLabel, text="Ahora mismo te encuentras en "+currentPosition)
    stateLabel = ttk.Label(mainLabel, text="ESTADO: "+ state)
    mainLabel.pack(padx=5, pady=5)
    infoLabel.pack(padx=5, pady=5)
    stateLabel.pack(padx=5, pady=5)
    return infoLabel, stateLabel

# DESCRIPTION: Actualiza los elementos de la interfaz gráfica
# STARTING_VALUES: Las etiquetas (Label) de la interfaz que muestran el estado y la posición del usuario
# RETURNS: NONE
# NEEDS: NONE
def updateGUI(root, infoLabel, stateLabel):
    global currentPosition, state
    if state == "Montado":
        infoLabel.config(text="Ahora mismo te encuentras en movimiento")
    else:
        infoLabel.config(text="Ahora mismo te encuentras en "+currentPosition)
    stateLabel.config(text="ESTADO: "+ state)

    root.after(1000, lambda: updateGUI(root, infoLabel, stateLabel))


#############################
##  Program starting point  #
#############################

if len(sys.argv) == 6:
        # Argument handling
        BROKER_IP = sys.argv[1]
        BROKER_PORT = sys.argv[2]
        CLIENT_ID = sys.argv[3]
        INITIAL_POSITION = str(sys.argv[4]).zfill(3)
        SERVER_IP = str(sys.argv[5])

        currentPosition = INITIAL_POSITION

        state = "Iniciada aplicación"
        try:
            data = readJson()

            # Graphical User Interface
            root = tk.Tk()
            root.title("EasyCab CUSTOMER Control Panel")
            mainFrame = ttk.Frame(root, padding="10")
            mainFrame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
            # Create tables for taxi and customer
            infoLabel, stateLabel = designInterface(mainFrame)



            mainThread = threading.Thread(target=programLoop)
            mainThread.daemon = True
            mainThread.start()

            root.after(1000, lambda: updateGUI(root, infoLabel, stateLabel))
            root.mainloop()
        except KeyboardInterrupt:
            print(f'[CUSTOMER APPLICATION] Application shutdown due to human interaction')
        except Exception as e:
            print(f"[CUSTOMER APPLICATION] THERE HAS BEEN AN ERROR. {e}")
        finally:
            print(f'EXITING CUSTOMER APPLICATION')


else:
    # We print the correct parameters to use
    print("SORRY, INCORRECT PARAMETER USE. \nUSAGE: <BROKER_IP> <BROKER_PORT> <CLIENT_ID> <IDPOS_OF_INITIAL_LOCATION> <IP_FOR_THE_CUSTOMER>")