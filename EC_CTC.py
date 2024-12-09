# import required modules
from flask import Flask, request, jsonify
import requests
import threading

weatherKeyFile = "OpenWeatherKey.txt"
# La URL base para la conexión de OpenWeather
BASE_API_URL = "http://api.openweathermap.org/data/2.5/weather?"

# Definimos las variables globales
global CITY, CITYLock

################  API DEFINITION  ################

app = Flask(__name__)

# DESCRIPTION: Este método espera peticiones API de la central
# STARTING_VALUES: NONE
# RETURNS: La respuesta en versión JSON
# NEEDS: NONE
@app.route('/getWeather', methods=['POST'])
def getWeather():
    response = callOpenWeather()
    return jsonify({"response": response})  # Respuesta en JSON

# DESCRIPTION: Este método actúa como wrapper para la llamada de API
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def apiWrapper():
    app.run(debug=True, port=5000)

# DESCRIPTION: Este método recupera la Key para la API de OpenWeather desde un archivo
# STARTING_VALUES: NONE
# RETURNS: La key en string
# NEEDS: NONE
def getOpenWeatherKey():

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
def callOpenWeather(CITY):
	
    try:
        requestURL = BASE_API_URL + "appid=" + getOpenWeatherKey() + "&q=" + CITY
        response = requests.get(requestURL)
        jsonResponse = response.json()
        if jsonResponse["cod"] != '404':
            x = jsonResponse["main"]
            cityTemperature = x["temp"] - 273.15
            if cityTemperature > 0:
                return 'OK'
            else:
                return 'KO'
        else:
            print("[OPENWEATHER CALLER] The selected city is invalid")
    except Exception as e:
        print(f'[OPENWEATHER CALLER] THERE HAS BEEN AN ERROR WHILE CALLING THE OPENWEATHER API. {e}')

# DESCRIPTION: Este método cambia la ciudad del sistema
# STARTING_VALUES: NONE
# RETURNS: NONE
# NEEDS: NONE
def changeCitySystem():

    try:
        # Declaramos las variables globales
        global CITY, CITYLock

        city = input("Introduzca la ciudad a la que quiere cambiar: ")
        CITYLock.acquire()
        CITY = city
        CITYLock.release()
    except KeyboardInterrupt:
        print(f"[CITY MANAGER] APPLICATION ENDED DUE TO HUMAN INTERACTION")
    except Exception as e:
        print(f"[CITY MANAGER] THERE HAS BEEN AN ERROR WHILE CHANGING THE CITY. {e}")

#########  MAIN STARTING POINT  #########
if __name__ == '__main__':
    
    CITYLock = threading.Lock()

    # Realizamos una llamada inicial para establecer la ciudad
    changeCitySystem()

    # Hilo que se encarga de enviar OK o KO a los componentes que llamen la API de EC_CTC
    threadgSendWeather = threading.Thread(target=apiWrapper)
    # Búcle para cambiar la ciudad
    while True:
        changeCitySystem()
