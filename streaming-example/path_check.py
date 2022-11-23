
import pathlib


airport_data = pathlib.Path.cwd()/'dataset'/'airports_csv'/'airports.csv'
airport_ouput_path = pathlib.Path.cwd()/'outputs'/'streaming-example'

flights_data = pathlib.Path.cwd()/'dataset'/'flights_json'/'flights.json'
flights_ouput_path = pathlib.Path.cwd()/'outputs'/'flights-data'


print(airport_data.exists())
print(airport_ouput_path.exists())
print(flights_data.exists())







