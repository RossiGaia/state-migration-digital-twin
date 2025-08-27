from connections import MqttConnection
import threading
import time

mqtt_connection = MqttConnection()

mqtt_t = threading.Thread(target=mqtt_connection.run)
mqtt_t.start()

time.sleep(3)

mqtt_connection.stop()
mqtt_t.join()