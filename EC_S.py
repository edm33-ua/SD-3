import socket
import sys
import time
from pynput import keyboard
import threading

global stop, connected
FORMAT = "utf-8"
HEADER = 1024
OK = "OK".encode(FORMAT)
KO = "KO".encode(FORMAT)

# DESCRIPTION: Envía el estado del sensor, según el valor de la variable stop
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def sendState():
    global connected
    try:
        if connected:
            # Send OK
            if not stop:
                client.send(OK)
            # Send KO
            else:
                client.send(KO)
    except Exception as e:
        print(f"[MESSAGE SENDER] SOMETHING WENT WRONG: {e}")
        print("[SENSOR APPLICATION] STOPPING SENSOR SERVICE...")
        connected = False
        client.close()

# DESCRIPTION: Espera un mensaje de confirmación de la recepción del estado por parte del Digital Engine
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def receive():
    global connected
    try:
        while connected:
            answer = client.recv(HEADER).decode(FORMAT)
            if answer:
                print(f"Message: {answer}")
    except Exception as e:
        print(f"[MESSAGE RECEIVER] SOMETHING WENT WRONG: {e}")
        print("[SENSOR APPLICATION] STOPPING SENSOR SERVICE...")
        connected = False
        client.close()  

# DESCRIPTION: Cambia el valor de stop a True
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def keyPressed(tecla):
    global stop
    stop = True
    print("[KEYBOARD LISTENER] Key was pressed")

# DESCRIPTION: Cambia el valor de stop a False
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def keyReleased(tecla):
    print("[KEYBOARD LISTENER] Key was released")
    global stop
    stop = False

# DESCRIPTION: Inicia la detección de pulsación de teclas
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def startListener():
    try:
        with keyboard.Listener(on_press=keyPressed, on_release=keyReleased) as listener:
            listener.join()
    # Manage any exception ocurred
    except Exception as e:
        print(f"[KEYBOARD LISTENER] AN ERROR OCURRED WHILE SENDING MY STATE: {e}")
        listener.close()
        

#############################
#   Program starting point  #
#############################
if  (len(sys.argv) == 3):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    stop = False
    connected = True
    try:
        # Begin the listener thread
        listenerThread = threading.Thread(target=startListener)
        listenerThread.daemon = True
        listenerThread.start()

        #Open up connection
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)

        thread = threading.Thread(target=receive)
        thread.daemon = True
        thread.start()
    
        while listenerThread.is_alive and connected:
            time.sleep(1)
            sendState()

    except ConnectionError:
        print(f"Unable to find DIGITAL ENGINE on {ADDR}")
        client.close()  
    except Exception as e:
        print(f"SOMETHING WENT WRONG: {e}")
        print("Stopping sensor service...")
        client.close()  
    except KeyboardInterrupt:
        print(f"[DIGITAL ENGINE] Application shutdown due to human interaction")
    finally:
        print(f'EXITING SENSOR APPLICATION')

else:
    print("SORRY, INCORRECT PARAMETER USE. \nUSAGE: <DIGITAL ENGINE IP> <PORT>")