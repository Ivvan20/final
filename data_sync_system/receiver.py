import paho.mqtt.client as mqtt
import json
from datetime import datetime
import logging
import sqlite3
import os
import sys

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================
def setup_logging():
    """Настраивает логирование для приёмника"""
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/receiver.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# =============================================================================
# НАСТРОЙКА ЦЕНТРАЛЬНОЙ БАЗЫ ДАННЫХ
# =============================================================================

def setup_central_database():
    """Создает центральную базу данных для приема данных"""
    try:
        conn = sqlite3.connect('central_storage.db', check_same_thread=False)
        cursor = conn.cursor()
        
        # Создаем таблицу для принятых данных
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS received_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id INTEGER,
                sensor_id INTEGER NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL,
                received_at TEXT NOT NULL,
                source TEXT,
                version TEXT
            )
        ''')
        conn.commit()
        logger.info("✅ Центральная база данных готова")
        return conn
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания центральной базы: {e}")
        raise

# =============================================================================
# MQTT НАСТРОЙКИ
# =============================================================================

MQTT_BROKER = "broker.hivemq.com"
MQTT_TOPIC = "my_school_project/sensor_data_v2"

# =============================================================================
# ОБРАБОТЧИКИ MQTT СОБЫТИЙ
# =============================================================================

def on_connect(client, userdata, flags, rc, properties=None):
    """Обработчик подключения к брокеру"""
    if rc == 0:
        logger.info("✅ ПРИЁМНИК ПОДКЛЮЧЕН К MQTT БРОКЕРУ!")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"📡 ПОДПИСКА НА ТОПИК: '{MQTT_TOPIC}'")
        logger.info("🎧 ОЖИДАНИЕ ДАННЫХ...")
    else:
        logger.error(f"❌ ОШИБКА ПОДКЛЮЧЕНИЯ. Код: {rc}")

def on_message(client, userdata, msg):
    """Обработчик входящих сообщений"""
    try:
        # Декодируем JSON сообщение
        payload = json.loads(msg.payload.decode())
        
        logger.info(f"\n📨 ПОЛУЧЕНО НОВОЕ СООБЩЕНИЕ")
        logger.info(f"├─ Время: {datetime.now().strftime('%H:%M:%S')}")
        logger.info(f"├─ Топик: {msg.topic}")
        logger.info(f"├─ Данные: {json.dumps(payload, indent=2)}")
        
        # Сохраняем в центральную базу
        save_to_database(payload)
        
        # Дублируем в лог-файл
        save_to_logfile(payload)
        
        logger.info("✅ ДАННЫЕ УСПЕШНО СОХРАНЕНЫ")

    except json.JSONDecodeError as e:
        logger.error(f"❌ ОШИБКА JSON: {e}")
    except Exception as e:
        logger.error(f"💥 ОШИБКА ОБРАБОТКИ: {e}")

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    """Обработчик успешной подписки"""
    logger.info(f"✅ УСПЕШНАЯ ПОДПИСКА. QoS: {granted_qos[0]}")

def on_disconnect(client, userdata, rc, properties=None):
    """Обработчик отключения от брокера"""
    if rc != 0:
        logger.warning("⚠️  НЕОЖИДАННОЕ ОТКЛЮЧЕНИЕ ОТ БРОКЕРА")

# =============================================================================
# ФУНКЦИИ СОХРАНЕНИЯ ДАННЫХ
# =============================================================================

def save_to_database(payload):
    """Сохраняет данные в SQLite базу"""
    try:
        conn = sqlite3.connect('central_storage.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO received_data 
            (original_id, sensor_id, value, timestamp, received_at, source, version) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            payload.get('id'),
            payload.get('sensor_id'),
            payload.get('value'),
            payload.get('timestamp'),
            datetime.now().isoformat(),
            payload.get('source', 'unknown'),
            payload.get('version', '1.0')
        ))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения в базу: {e}")

def save_to_logfile(payload):
    """Сохраняет данные в текстовый лог-файл"""
    try:
        with open("received_data.log", "a", encoding="utf-8") as f:
            log_entry = {
                "received_at": datetime.now().isoformat(),
                "data": payload
            }
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error(f"❌ Ошибка записи в лог-файл: {e}")

# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ ПРИЁМНИКА
# =============================================================================

def main():
    """Основная функция приёмника"""
    logger.info("🚀 ЗАПУСК СИСТЕМЫ ПРИЁМА ДАННЫХ")
    logger.info("=" * 50)
    
    client = None
    
    try:
        # Инициализация базы данных
        setup_central_database()
        
        # Создание MQTT клиента
        client_id = f"receiver_{datetime.now().strftime('%H%M%S')}"
        client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        
        # Назначение обработчиков событий
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_subscribe = on_subscribe
        client.on_disconnect = on_disconnect

        # Подключение к брокеру
        logger.info(f"🔗 ПОДКЛЮЧЕНИЕ К БРОКЕРУ {MQTT_BROKER}...")
        client.connect(MQTT_BROKER, 1883, 60)

        # Запуск бесконечного цикла
        logger.info("🔄 ЗАПУСК ПРОСЛУШИВАНИЯ...")
        client.loop_forever()
        
    except KeyboardInterrupt:
        logger.info("\n🛑 ПРИЁМНИК ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ")
    except Exception as e:
        logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
    finally:
        if client:
            client.disconnect()
        logger.info("🎯 ПРИЁМНИК ЗАВЕРШИЛ РАБОТУ")

if __name__ == "__main__":
    main()