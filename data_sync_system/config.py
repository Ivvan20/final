# config.py - Конфигурация базы данных
DATABASE_CONFIG = {
    'sqlite': {
        'type': 'sqlite',
        'database': 'local_sensor_data.db'
    },
    'mysql': {
        'type': 'mysql',
        'host': 'localhost',
        'user': 'root',
        'password': 'password',
        'database': 'sensor_data',
        'port': 3306
    }
}

# Активная база данных ('sqlite' или 'mysql')
ACTIVE_DATABASE = 'sqlite'

# MQTT настройки
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "my_school_project/sensor_data_v3"