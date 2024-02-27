import os
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
from time import sleep

# Location coordinates
NEWYORK_MANHATTAN_TIMESQUARE_COORDINATES = {"latitude": 40.758896, "longitude": -73.985130}
NEWYORK_MANHATTAN_SOHO_COORDINATES = {"latitude": 40.7215, "longitude": -74.0000}
NEWYORK_QUEENS_FLUSHING_COORDINATES = {"latitude": 40.7658, "longitude": -73.8331}
NEWYORK_BROOKLYN_BEDSTUY_COORDINATES = {"latitude": 40.6833, "longitude": -73.9380}
MASSACHESETTS_BOSTON_SOUTHEND_COORDINATES = {"latitude": 42.3396, "longitude": -71.0698}
MASSACHESETTS_CAMBRIDGE_COORDINATES = {"latitude": 42.374443, "longitude": -71.116943}
MASSACHESETTS_MALDEN_COORDINATES = {"latitude": 42.429752, "longitude": -71.071022}

# Start location and destination variables
random.seed(42)
start = NEWYORK_MANHATTAN_TIMESQUARE_COORDINATES
destination = MASSACHESETTS_BOSTON_SOUTHEND_COORDINATES
start_location = start.copy()
start_time = datetime.now()

# Calculate movement increments
LATITUDE_INCREMENT = abs((abs(start['latitude'])
                      - abs(destination['latitude']))/100)

LONGITUDE_INCREMENT = abs((abs(start['longitude'])
                      - abs(destination['longitude']))/100)

# Environmental variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

# def generate_car_make_and_model():
#     car_models = {
#         'Toyota': ['Camry', 'Corolla', 'Rav4', 'Prius', 'Highlander'],
#         'Honda': ['Accord', 'Civic', 'CR-V', 'Pilot', 'Odyssey'],
#         'Ford': ['F-150', 'Focus', 'Escape', 'Explorer', 'Mustang'],
#         'Chevrolet': ['Silverado', 'Equinox', 'Malibu', 'Tahoe', 'Camaro'],
#     }
#
#     car_make = random.choice(list(car_models.keys()))
#     car_model = random.choice(car_models[car_make])
#
#     return car_make, car_model

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_gps_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': (location[0], location[1]),
        'speed': random.uniform(15, 65),  # in mph
    }

def simulate_vehicle_movement():
    global start_location

    # move toward destination
    # see which direction we have to move
    if(start_location['latitude'] < destination['latitude']):
        if(start_location['longitude'] < destination['longitude']):
            start_location['latitude'] += LATITUDE_INCREMENT
            start_location['longitude'] += LONGITUDE_INCREMENT
            # add some randomness to simulate actual road travel
            start_location['latitude'] += random.uniform(-0.0005, 0.0005)
            start_location['longitude'] += random.uniform(-0.0005, 0.0005)
        else: # start_location['longitude'] > destination['longitude']
            start_location['latitude'] += LATITUDE_INCREMENT
            start_location['longitude'] -= LONGITUDE_INCREMENT
            start_location['latitude'] += random.uniform(-0.0005, 0.0005)
            start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    else: # start_location['latitude'] > destination['latitude']
        if (start_location['longitude'] < destination['longitude']):
            start_location['latitude'] -= LATITUDE_INCREMENT
            start_location['longitude'] += LONGITUDE_INCREMENT
            start_location['latitude'] += random.uniform(-0.0005, 0.0005)
            start_location['longitude'] += random.uniform(-0.0005, 0.0005)
        else:  # start_location['longitude'] > destination['longitude']
            start_location['latitude'] -= LATITUDE_INCREMENT
            start_location['longitude'] -= LONGITUDE_INCREMENT
            start_location['latitude'] += random.uniform(-0.0005, 0.0005)
            start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    # random_make, random_model = generate_car_make_and_model()

    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(15, 65), # in mph
        'make': 'Porsche',
        'model': '911',
        'year': random.randint(2014, 2024)
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': (location[0], location[1]),
        'camera_id': camera_id,
        'snapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': (location[0], location[1]),
        'temperature': random.uniform(0, 100),
        'weather_condition': random.choice(['Sunny', 'Mostly Sunny', 'Cloudy', 'Partly Cloudy', 'Showers', 'Rain', 'Heavy Rain', 'Thunderstorm', 'Light Snow', 'Snow', 'Heavy Snow', 'Hail']),
        'precipitation': random.randint(0, 100), # percentage
        'humidity': random.randint(0, 100), # percentage
        'wind_speed': random.uniform(0, 30), # mph
        'air_quality': random.uniform(0, 500) # aqi
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': (location[0], location[1]),
        'incident_id': uuid.uuid4(),
        'incident_type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
   producer.produce(
       topic,
       key=str(data['id']),
       value=json.dumps(data, default=json_serializer).encode('utf-8'),
       on_delivery=delivery_report
   )

   producer.flush()


def simulate_journey(producer, device_id):
    count = 0
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'Camera-1')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        # print('vehicle data')
        # print(vehicle_data)
        # print('gps data')
        # print(gps_data)
        # print('traffic camera data')
        # print(traffic_camera_data)
        # print('weather data')
        # print(weather_data)
        # print('emergency incident data')
        # print(emergency_incident_data)
        count += 1
        print(count)

        if (abs(vehicle_data['location'][0] - destination['latitude']) <= 0.02 and abs(vehicle_data['location'][1] - destination['longitude']) <= 0.02):
            print('Vehicle has reached destination. Simulation ending...')
            break

        print(vehicle_data['location'])
        print(destination)

        if count > 200:
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Sedan-1')
    except KeyboardInterrupt:
        print('Interrupted by the user')
    except Exception as e:
        print(f'Exception: {e}')