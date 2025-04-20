import requests, json
from datetime import datetime, timedelta, timezone

# Gotten from openweather map
api_key = "620c3bceb247a0316637fcdca085a4ad"
base_url = "http://api.openweathermap.org/data/2.5/weather?" #Base URL for weather
city_name = "Portland" 

forecast_url = "http://api.openweathermap.org/data/2.5/forecast" #URL for forecast
params = {#Params for forecast
    "q" : city_name,
    "appid": api_key
}


complete_url = base_url + "appid=" + api_key + "&q=" + city_name #Base URL
response = requests.get(complete_url)
# Convert json format data into python format data
x = response.json()

forecast_response = requests.get(forecast_url, params=params) #Request using (url, params)
forecast_data = forecast_response.json()

# If OK for current weather
if x.get("cod") == 200:
    y = x["main"] # Store the value of "main" in variable

    current_temperature = y["temp"]
    current_pressure = y["pressure"]
    current_humidity = y["humidity"]
    z = x["weather"]
    weather_descritpion = z[0]["description"]

    weather_main = z[0]["main"]
    if weather_main.lower() in ["rain", "rainy", "raining"]:
        print("Yes, it is currently raining in Portalnd right now.")
    else:
        print("No, it is not raining in Portland right now.")

    print("Temperature (in kelvin) = " + str(current_temperature) +
          "\n atmospheric pressure (in hPa) = " + str(current_pressure) + 
          "\n humidity (in percentage) = " + str(current_humidity) + 
          "\n description = " + str(weather_descritpion))
    
else:
    print("City Not Found")

rain_expected = False
now = datetime.now(timezone.utc)
three_days_later = now + timedelta(days=3)

#If OK for forecast.
if forecast_data.get("cod") == "200":
    for entry in forecast_data["list"]:
        weather_main = entry["weather"][0]["main"].lower()
        forecast_time = datetime.strptime(entry["dt_txt"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        if(forecast_time > three_days_later):
            continue #Only consider next 3 days.

        if weather_main in ["rain", "rainy", "raining"]:
            print(f"Rain is forecasted in Portland at {forecast_time}!")
            rain_expected = True
            break

else:
    print("Forecast data not found")

if not rain_expected:
    print("No rain forecasted in the next 3 days.")
