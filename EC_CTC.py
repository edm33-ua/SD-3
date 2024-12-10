from flask import Flask, request, jsonify
import requests
import threading
import tkinter as tk

# El documento con la KEY para usar OpenWeather
weatherKeyFile = "OpenWeatherKey.txt"
# La URL base para la conexión de OpenWeather
BASE_API_URL = "http://api.openweathermap.org/data/2.5/weather?"

# Definimos las variables globales
City = 'Madrid'
Temp = '-'
################  CHANGE CITY  #################

# DESCRIPTION: Este método se encarga de actualizar el valor de la ciudad con el de la caja de texto del menú, y la temperatura por la
# correspondiente de su ciudad
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def updateCity():
    try:
        # We define the global variables
        global textbox, cityLabel, City, Temp, tempLabel
        # Then we assign the value and delete the residue from the textbox
        City = textbox.get()
        textbox.delete(first=0, last="end")
        # We get the new temperature
        callOpenWeather()
        cityLabel.config(text=f"Ciudad seleccionada: {City}")
        tempLabel.config(text=f"Temperatura: {round(Temp,2)}")
    except Exception as e:
        print(f'[MENU HANDLER] THERE HAS BEEN AN ERROR WHILE UPDATING THE CITY. {e}')

# DESCRIPTION: Este método se encarga de generar la ventana con el menú para cambiar el sistema
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: updateCity()
def windowMenu():
    try:
        global textbox, cityLabel, tempLabel
        # We create the main window
        window = tk.Tk()
        window.title("Selector de Ciudad")
        window.geometry("600x400")
        window.config(padx=20, pady=60)

        # We create the label to show the selected city
        cityLabel = tk.Label(window, text="Ciudad seleccionada: Ninguna", font=("Times New Roman", 16))
        cityLabel.pack(pady=10)

        # We create the label to show the last asked temperature
        tempLabel = tk.Label(window, text=f"Temperatura: -", font=('Times New Roman', 16))
        tempLabel.pack(pady=10)

        # We create the textbox where we'll introduce the new city
        textbox = tk.Entry(window, font=("Times New Roman Greek", 12), width=20)
        textbox.pack(pady=10)

        # We create the button to upload the city value
        button = tk.Button(window, text="Actualizar Ciudad", command=updateCity, font=("Times New Roman Baltic", 14))
        button.pack(pady=10)

        # We begin the loop with the main window
        window.mainloop()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f'[MENU HANDLER] THERE HAS BEEN AN ERROR WHILE CREATING THE MENU. {e}')

# DESCRIPTION: Este método se encarga de generar el menú para cambiar la ciudad del sistema y facilitar dicho proceso
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: windowMenu()
def changeCitySystem():
    try:
        windowMenu()
    except KeyboardInterrupt:
        print(f"[CITY MANAGER] APPLICATION ENDED DUE TO HUMAN INTERACTION")
    except Exception as e:
        print(f"[CITY MANAGER] THERE HAS BEEN AN ERROR WHILE MANTAINING THE CITY. {e}")

###############  OPENWEATHER OPERATIONS  ###############

# DESCRIPTION: Este método recupera la Key para la API de OpenWeather desde un archivo
# STARTING_VALUES: NONE
# RETURNS: La key en string
# NEEDS: NONE
def getOpenWeatherKey():
    # Declaramos las variables globales
    global weatherKeyFile
    try:
        # Abrimos el archivo, leemos la key, y lo cerramos
        file = open(weatherKeyFile)
        key = file.readline()
        file.close()
        return key
    except Exception as e:
        print(f"[OPENWEATHER KEY] THERE HAS BEEN AN ERROR WHILE READING THE KEY. {e}")

# DESCRIPTION: Este método realiza una llamada a OpenWeather con la ciudad introducida
# STARTING_VALUES: NONE
# RETURNS: OK o KO
# NEEDS: getOpenWeatherKey()
def callOpenWeather():
	# Declaramos las variables globales
    global City, BASE_API_URL, Temp
    try:
        requestURL = BASE_API_URL + "appid=" + getOpenWeatherKey() + "&q=" + City
        response = requests.get(requestURL)
        jsonResponse = response.json()
        if jsonResponse["cod"] != '404':
            x = jsonResponse["main"]
            cityTemperature = x["temp"] - 273.15
            Temp = cityTemperature
            if cityTemperature > 0:
                return 'OK'
            else:
                return 'KO'
        else:
            print("[OPENWEATHER CALLER] The selected city is invalid")
            return 'ERR'
    except Exception as e:
        print(f'[OPENWEATHER CALLER] THERE HAS BEEN AN ERROR WHILE CALLING THE OPENWEATHER API. {e}')

################  API DEFINITION  ################

app = Flask(__name__)

# DESCRIPTION: Este método espera peticiones API de la central
# STARTING_VALUES: NONE
# RETURNS: La respuesta en versión JSON
# NEEDS: NONE
@app.route('/getWeather', methods=['GET'])
def getWeather():
    global City
    response = callOpenWeather()
    return jsonify({"response": response})  # Respuesta en JSON

#########  MAIN STARTING POINT  #########
if __name__ == '__main__':

    # We create the thread for the city menu
    thread=threading.Thread(target=changeCitySystem)
    thread.daemon = True
    thread.start()

    # We begin running the API
    try:
        app.run()
    except KeyboardInterrupt:
        print(f"[APP HANDLER] APPLICATION ENDED DUE TO HUMAN INTERACTION")
    except Exception as e:
        print(f"[APP HANDLER] THERE HAS BEEN AN ERROR WHILE RUNNING THE APP. {e}")
    
