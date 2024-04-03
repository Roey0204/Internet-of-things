import sys
sys.path.append('..')
from lib.Mongo import MyMongo
from lib.Sensor import DHT11Sensor

def main():
    # Init different collection in DB
    collection_temp = MyMongo()
    collection_humi = MyMongo()

    # Connect to collection and db
    collection_temp.connect("temperature")
    collection_humi.connect("humidity")

    # Meta Data for temperature
    sensor_id    = 1
    sensor_type  = "temperature"
    sensor_unit  = "celcius"
        
    sensor_info  = [sensor_id,sensor_type,sensor_unit]

    # Meta Data for humidity
    sensor_id2    = 1
    sensor_type2  = "humidity"
    sensor_unit2  = "percentage"
        
    sensor_info2  = [sensor_id2,sensor_type2,sensor_unit2]

    dht_sensor = DHT11Sensor(pin=16)

    while True:
        temp,humi=dht_sensor.get_temperature_humidity()

        if temp is not None and humi is not None:

            doc_list  = [sensor_info,temp]
            doc_list2 = [sensor_info2,humi]

            collection_temp.create_document(doc_list)
            collection_humi.create_document(doc_list2)

            print(f"Temperature Reading: {temp} , Celcius Humidity: {humi}%")
        
# Main Run
if __name__ == "__main__":
    main()

            