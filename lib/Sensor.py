'''
This module is to acquire data from DHT11
'''
import RPi.GPIO as GPIO
import dht11
import time
from queue import Queue
from threading import Thread

class DHT11Sensor:
    def __init__(self, pin=4):

        # initialize GPIO
        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)
        GPIO.cleanup()

        # initialize DHT11 sensor
        self.instance = dht11.DHT11(pin=pin)

        # Queue for storing sensor data
        self.data_queue = Queue()

        # Start a thread to continuously read sensor data
        self.sensor_thread = Thread(target=self.read_sensor_data)
        self.sensor_thread.daemon = True
        self.sensor_thread.start()

    # Function to read sensor data and put it into the queue
    def read_sensor_data(self):
        while True:
            result = self.instance.read()
            if result.is_valid():
                self.data_queue.put(result.temperature)
                self.data_queue.put(result.humidity)
            time.sleep(0.5)  # Read sensor data every 2 seconds

    # Method to get temperature and humidity from the queue
    def get_temperature_humidity(self):
        if not self.data_queue.empty():
            temperature = self.data_queue.get()
            humidity = self.data_queue.get()
            return temperature, humidity
        else:
            return None, None
        
if __name__ == "__main__":
    dht_sensor = DHT11Sensor(pin=16)

    while True:
        temp,humi=dht_sensor.get_temperature_humidity()
        if temp is not None and humi is not None:
            print(temp,humi)
        time.sleep(1)
