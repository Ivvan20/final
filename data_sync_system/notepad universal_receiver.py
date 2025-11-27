import sys
import os
import json
from datetime import datetime
import logging
import paho.mqtt.client as mqtt

sys.path.append('C:\\Users\\Student\\AppData\\Roaming\\Python\\Python313\\site-packages')

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

import sqlite3

try:
    from config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC
except ImportError:
    print("❌ Файл config.py не найден! Создайте его сначала.")
    sys.exit(1)

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================
def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/universal_receiver.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# =============================================================================
# УНИВЕРСАЛЬНОЕ ХРАНИЛИЩЕ ДАННЫХ
# =============================================================================

class CentralStorage:
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Подключается к центральному хранилищу (SQLite)"""
        try:
            self.connection = sqlite3.connect('central_universal.db', check_same_thread=False)
            self.cursor = self.connection.cursor()
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS received_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_id INTEGER,
                    sensor_id INTEGER NOT NULL,
                    value REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    source_db TEXT,
                    db_type TEXT,
                    version TEXT
                )
            ''')
            self.connection.commit()
            logger.info("✅ Центральное хранилище готово")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка центрального хранилища: {e}")
            return False
    
    def save_data(self, payload):
        """Сохраняет полученные данные"""
        try:
            self.cursor.execute('''
                INSERT INTO received_data 
                (original_id, sensor_id, value, timestamp, received_at, source_db, db_type, version) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                payload.get('id'),
                payload.get('sensor_id'),
                payload.get('value'),
                payload.get('timestamp'),
                datetime.now().isoformat(),
                payload.get('source'),
                payload.get('database_type'),
                payload.get('version')
            ))
            self.connection.commit()
            
            logger.info(f"💾 Сохранено в центральное хранилище: ID {payload.get('id')}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")
            return False
    
    def close(self):
        if self.connection:
            self.connection.close()

# =============================================================================
# MQTT КЛИЕНТ
# =============================================================================

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        logger.info("✅ УНИВЕРСАЛЬНЫЙ ПРИЁМНИК ПОДКЛЮЧЕН!")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"📡 Подписка на топик: '{MQTT_TOPIC}'")
    else:
        logger.error(f"❌ Ошибка подключения. Код: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        
        logger.info(f"\n📨 ПОЛУЧЕНО СООБЩЕНИЕ ИЗ {payload.get('database_type', 'unknown').upper()}")
        logger.info(f"├─ Источник: {payload.get('source')}")
        logger.info(f"├─ ID записи: {payload.get('id')}")
        logger.info(f"├─ Датчик: {payload.get('sensor_id')}")
        logger.info(f"├─ Значение: {payload.get('value')}°C")
        logger.info(f"└─ Версия: {payload.get('version')}")

        # Сохраняем в центральное хранилище
        storage.save_data(payload)
        
        # Дублируем в лог
        with open("received_universal.log", "a", encoding="utf-8") as f:
            log_entry = {
                "received_at": datetime.now().isoformat(),
                "data": payload
            }
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            
        logger.info("✅ Данные успешно обработаны")

    except Exception as e:
        logger.error(f"💥 Ошибка обработки: {e}")

# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    global storage
    logger.info("🚀 УНИВЕРСАЛЬНЫЙ ПРИЁМНИК ЗАПУЩЕН")
    logger.info("=" * 50)
    
    client = None
    storage = CentralStorage()
    
    try:
        if not storage.connect():
            return
        
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "universal_receiver")
        client.on_connect = on_connect
        client.on_message = on_message

        logger.info(f"🔗 Подключение к {MQTT_BROKER}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        logger.info("🎧 Ожидание данных из различных СУБД...")
        client.loop_forever()
        
    except KeyboardInterrupt:
        logger.info("\n🛑 ПРИЁМНИК ОСТАНОВЛЕН")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        if client:
            client.disconnect()
        storage.close()
        logger.info("🎯 ПРИЁМНИК ЗАВЕРШИЛ РАБОТУ")

if __name__ == "__main__":
    main()