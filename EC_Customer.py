from kafka import KafkaConsumer, KafkaProducer
import sys
import json
import time

FORMAT = 'utf-8'
SECONDS = 5

global currentPosition

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
    global currentPosition
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
                    return True
                elif 'FINISHED' in decodedMessage:
                    print(f'[SERVICE MANAGEMENT] Your taxi has arrived to the destination. Please dismount from the vehicle')
                    # Received message format: <CLIENTID>_FINISHED_<LOCATION>
                    # Example: a_FINISHED_129
                    currentPosition = decodedMessage[11:]
                    time.sleep(1)
                    producer.send('Customer2CentralACK', (CLIENT_ID + '_ACK').encode(FORMAT))
                    return False
                elif 'BROKE' in decodedMessage:
                    print(f'[SERVICE MANAGEMENT] Your taxi has broken down. We\'re sorry, but here ends your journey')
                    # Received message format: <CLIENTID>_BROKE_<LOCATION>
                    # Example: a_BROKE_129
                    currentPosition = decodedMessage[11:]
                    return None
                else:
                    print(f'[SERVICE MANAGEMENT] ERROR: UNEXPECTED MESSAGE FROM CENTRAL CONTROL')
        
    # We catch and print any exceptions in case they happen
    except Exception as e:
            print(f'[SERVICE MANAGEMENT] THERE HAS BEEN AN ERROR WHEN LISTENING FOR CENTRAL MESSAGES: {e}')
    finally:
        # We close both the consumer and producer in case there is an error
        consumer.close()
        producer.close()
     

# DESCRIPTION: Este método pregunta a la Central si hay algún taxi disponible
# y devuelve True o False según la respuesta de la central
# STARTING_VALUES: NONE
# RETURNS: True si lo hay y False, si no
# NEEDS: NONE
def askForTaxi(destination):
    try:
        # We create the kafka producer and the consumer
        producer = KafkaProducer(bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT))
        consumer = KafkaConsumer('Central2CustomerInformation', bootstrap_servers=str(BROKER_IP)+':'+str(BROKER_PORT), consumer_timeout_ms=SECONDS*1000)
        
        answered = False
        while not answered:
            # We ask for a taxi through the kafka producer
            print(f'[PETITION MANAGEMENT] Asking Central Control for a service to {destination}')
            producer.send('Customer2CentralPetitions', (CLIENT_ID + '_ASK_' + currentPosition.zfill(3) + "_" + destination).encode(FORMAT))
            # For each message that has arrived since we've created the kafka consumer, we will check if it has
            # any important information regarding our client
            for message in consumer:
                # We get a decoded version of the message value from kafka
                decodedMessage = message.value.decode(FORMAT)
                # If our ID is in the message
                if CLIENT_ID in decodedMessage:
                    answered = True
                    # then search if the substring informing about the taxi situation is in there
                    # If it's in there, we return the ID of the taxi,
                    if "OK" in decodedMessage:
                        print(f'[PETITION MANAGEMENT] Service to {destination} accepted by Central Control')
                        time.sleep(1)
                        producer.send('Customer2CentralACK', (CLIENT_ID + '_ACK').encode(FORMAT))
                        # We close both the consumer and producer in case there is an error
                        consumer.close()
                        producer.close()
                        return True
                    # but if there aren't any, we return NaN, which will log off the client
                    elif 'KO' in decodedMessage:
                        print(f'[PETITION MANAGEMENT] Service to {destination} rejected by Central Control')
                        producer.send('Customer2CentralACK', (CLIENT_ID + '_ACK').encode(FORMAT))
                        # We close both the consumer and producer in case there is an error
                        consumer.close()
                        producer.close()
                        return False
            print(f'[PETITION MANAGEMENT] THERE HAS BEEN NO RESPONSE FROM CENTRAL CONTROL')
            print(f'[PETITION MANAGEMENT] SENDING PETITION AGAIN')
    # We catch and print any exceptions in case they happen
    except Exception as e:
        print(f'[PETITION MANAGEMENT] THERE HAS BEEN AN ERROR WHILE ASKING FOR A TAXI: {e}, PLEASE TRY AGAIN LATER')
    finally:
        # We close both the consumer and producer in case there is an error
        consumer.close()
        producer.close()
        



#############################
##  Program starting point  #
#############################

if len(sys.argv) == 5:
        # Argument handling
        BROKER_IP = sys.argv[1]
        BROKER_PORT = sys.argv[2]
        CLIENT_ID = sys.argv[3]
        INITIAL_POSITION = str(sys.argv[4]).zfill(3)

        currentPosition = INITIAL_POSITION

        try:
            data = readJson()
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
            
        except KeyboardInterrupt:
            print(f'[CUSTOMER APPLICATION] Application shutdown due to human interaction')
        except Exception as e:
            print(f"[CUSTOMER APPLICATION] THERE HAS BEEN AN ERROR. {e}")
        finally:
            print(f'EXITING CUSTOMER APPLICATION')


else:
    # We print the correct parameters to use
    print("SORRY, INCORRECT PARAMETER USE. \nUSAGE: <BROKER_IP> <BROKER_PORT> <CLIENT_ID> <IDPOS_OF_INITIAL_LOCATION>")