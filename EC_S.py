import socket
import sys
import time
from pynput import keyboard
import threading
import tkinter as tk
from tkinter import ttk

global stop, connected
FORMAT = "utf-8"
HEADER = 1024
OK = "OK".encode(FORMAT)
KO = "KO".encode(FORMAT)

# DESCRIPTION: Envía de forma contínua el estado del sensor, según el valor de la variable stop
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def sendState():
    global connected, stop
    try:
        while True:
            # if connected:
            # Send OK
            if not stop:
                client.send(OK)
            # Send KO
            else:
                client.send(KO)
            time.sleep(1)
            print(f"Sent: {stop}")
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

# DESCRIPTION: Cambia el estado del sensor
# STARTING_VALUES: el marco principal de la aplicación
# RETURNS: NONE
# NEEDS: NONE
def changeState(button, stateLabel):
    global stop
    stop = not stop
    currentState = "OK"
    buttonText = "Parar"
        
    if stop or not connected:
        currentState = "KO"
        buttonText = "Iniciar"


    button.config(text=buttonText)
    stateLabel.config(text=currentState)

# DESCRIPTION: Actualiza los elementos de la interfaz gráfica
# STARTING_VALUES: el marco principal de la aplicación
# RETURNS: NONE
# NEEDS: NONE
def updateGUI(root, button, stateLabel):
    #For updating the GUI in the future
        
    root.after(1000, lambda: updateGUI(root, button, stateLabel))
      
# DESCRIPTION: Crea los elementos de la interfaz gráfica
# STARTING_VALUES: el marco principal de la aplicación
# RETURNS: NONE
# NEEDS: NONE
def designInterface(mainFrame):
    global stop, connected
    buttonFrame = ttk.Frame(mainFrame)
    buttonFrame.grid(row=1, column=0, padx=10, pady=10)

    buttonLabel = ttk.LabelFrame(buttonFrame, text="ESTADO DEL SENSOR "+ str(ID))
    currentState="OK"
    if stop or not connected:
        currentState = "KO"
    connectionLabel = ttk.Label(buttonLabel, text="Conectado a "+ str(SERVER) + ":" +  str(PORT))
    stateLabel = ttk.Label(buttonLabel, text=currentState)
    buttonLabel.pack(padx=5, pady=5)
    connectionLabel.pack(padx=5, pady=5)
    stateLabel.pack(padx=5, pady=5)
    

    buttonText = "Iniciar" if stop else "Parar"
    button = ttk.Button(buttonLabel, text=buttonText, command=lambda: changeState(button, stateLabel))
    button.pack(side=tk.BOTTOM, padx=5)

    return button, stateLabel



#############################
#   Program starting point  #
#############################
if  (len(sys.argv) == 4):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ID = str(sys.argv[3])
    ADDR = (SERVER, PORT)
    stop = False
    connected = True
    try:
        # Begin the listener thread
        # listenerThread = threading.Thread(target=startListener)
        # listenerThread.daemon = True
        # listenerThread.start()

        # Open up connection
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        
        # Start to process received data
        thread = threading.Thread(target=receive)
        thread.daemon = True
        thread.start()
    
        # Starting thread for sending state
        thread = threading.Thread(target=sendState)
        thread.daemon = True
        thread.start()
        # while listenerThread.is_alive and connected:
        #     time.sleep(1)
        #     sendState()

        # Graphical User Interface
        root = tk.Tk()
        root.title("EasyCab Central Control Panel")
        mainFrame = ttk.Frame(root, padding="10")
        mainFrame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        # Create tables for taxi and customer
        button = designInterface(mainFrame)


        # root.after(1000, lambda: updateGUI(root, button))

        root.mainloop()

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
    print("SORRY, INCORRECT PARAMETER USE. \nUSAGE: <DIGITAL ENGINE IP> <PORT> <SENSOR ID>")