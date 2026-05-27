1. промты скриптов в папке scripts
2. sql таблицы промтом или просто текстом для генерации
3. тест кафка промтом или кодом в папке test_kafka
4. report.ipynb в текстовом виде или ещё как то 
5. диаграмма либо в txt и потом сам скопирую поменяю расширенеие либо промтом, но лучше txt


Установка minio:
	установка:
	wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio_20250907161309.0.0_amd64.deb  -O minio.deb
	распаковка:
	sudo dpkg -i minio.deb
	проверка версии:
	minio --version
	создание папки:
	sudo mkdir -p /usr/local/share/minio
	экспорт конфигурации:
	export MINIO_ROOT_USER=minioadmin
	export MINIO_ROOT_PASSWORD=minioadmin


Установка clickhouse:
	установка:
	sudo apt-get install -y apt-transport-https ca-certificates dirmngr
	
	sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
	echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
	sudo apt-get update
	sudo apt-get install -y clickhouse-server clickhouse-client
	
	
Установка metabase:
	установка metabase:
	sudo mkdir -p /opt/metabase
	cd /opt/metabase
	sudo wget https://downloads.metabase.com/v0.56.9/metabase.jar
	драйвера для clickhouse:
	sudo mkdir -p /opt/metabase/plugins
	cd /opt/metabase/plugins
	sudo wget https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.53.4/clickhouse.metabase-driver.jar
	первый запуск:
	cd /opt/metabase
	sudo java -jar metabase.jar


Установка Airflow:
	sudo apt update && sudo apt upgrade -y
	sudo apt install python3-pip python3-dev build-essential -y
	mkdir ~/airflow_desktop && cd ~/airflow_desktop
	python3 -m venv airflow_venv
	source airflow_venv/bin/activate
	установка airflow:
	pip install --upgrade pip
	pip install psycopg2-binary
	pip install "apache-airflow[celery,postgres]==3.2.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.12.txt"
	проверка версии:
	airflow version
	плагин airflow для clickhouse:
	pip install 'apache-airflow[cncf.kubernetes]' 'clickhouse-driver>=0.2.6' 'airflow-clickhouse-plugin'
	pip install airflow-clickhouse-plugin clickhouse-driver
	установка дополнительных библиотек:
	pip install clickhouse-driver pandas numpy boto3 kafka-python pyspark

Усnановка kafka:
	# Переход в домашнюю директорию
	cd ~
	# Скачать Kafka 4.2.0 (Scala 2.13)
	wget https://dlcdn.apache.org/kafka/4.2.0/kafka_2.13-4.2.0.tgz
	# Распаковать
	tar -xzf kafka_2.13-4.2.0.tgz
	# Переименовать папку для удобства (как у вас — ~/kafka)
	mv kafka_2.13-4.2.0 kafka
	# Назначить права
	sudo chown -R $USER:$USER ~/kafka

Установка java:
	sudo apt update
	sudo apt install -y wget tar gzip
	# Установите Java 17 (или Java 21)
	sudo apt install -y openjdk-17-jdk
	# Проверка
	java -version
	Если openjdk-17-jdk не находится, добавьте PPA:
	sudo apt update
	sudo apt install -y wget tar gzip
	# Установите Java 17 (или Java 21)
	sudo apt install -y openjdk-17-jdk
	# Проверка
	java -version
=======================================================

Запуск minio
minio server /usr/local/share/minio --address :9001 --console-address :9002 &
	остановка minio:
	pkill minio

Запустить кафку
~/kafka/bin/kafka-storage.sh random-uuid
~/kafka/bin/kafka-storage.sh format -t <...> -c ~/kafka/config/server.properties --standalone
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
	проверка данных:
	~/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic raw_detections
	если топик не найден:
	~/kafka/bin/kafka-topics.sh --create --topic raw_detections --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
	проверка есть ли топик:
	~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
	на всякий случай:
	~/kafka/bin/kafka-topics.sh --create --topic raw_detections --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  	удаление данных из кафки возрастом больше 5 минут:
	~/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name raw_detections --alter --add-config retention.ms=300000
	права на папку kafka:
	sudo chown -R $USER:$USER ~/kafka
	остановить kafka:
	~/kafka/bin/kafka-server-stop.sh

Запустить clickhouse
sudo service clickhouse-server start
или
sudo systemctl start clickhouse-server
	проверка статуса:
	sudo systemctl status clickhouse-server
	создание таблиц:
	clickhouse-client --password user --multiquery < create_gold_tables.sql
clickhouse-client --host localhost --port 9000 --user default
создать таблицу
CREATE TABLE IF NOT EXISTS fact_camera_detections_realtime (
    camera_id String,
    window_start DateTime,
    window_end DateTime,
    vehicle_type_id UInt8,
    vehicle_count UInt32,
    unique_vehicles UInt32,
    avg_speed_kmh Float32,
    processing_time DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (camera_id, window_start, vehicle_type_id);
или 
clickhouse-client --host localhost --user default --password user --multiline < ~/data_analitick_3_11/moduls/1_A/dwh_gold_clickhouse.sql
Проверить создание:
SHOW TABLES;
Подключиться к бд:
USE transport;
Проверка количества записей в таблице:
SELECT count() FROM gold_incidents;
Последние 10 записей:
SELECT * FROM gold_incidents ORDER BY timestamp DESC LIMIT 10;
Проверить таблицы:
SHOW DATABASES;
Удалить базу данных:
DROP DATABASE имя_базы;
Удалить таблицу:
DROP TABLE [IF EXISTS] [db_name.]table_name;


Запустить metabase:
	запуска metabase:
	cd /opt/metabase
	sudo java -jar metabase.jar
	подключение:
	http://localhost:3000
	остановка:
	sudo pkill -f metabase.jar

Запуск airflow:
	переход в папку:
	cd ~/airflow_desktop
	активация окружения:
	source airflow_venv/bin/activate
	запуск airflow:
	airflow standalone
	найти и посмотреть логин и пароль:
	/home/user/airflow/simple_auth_manager_passwords.json.generated
ТЕРМИНАЛ ЗАКРЕПЛЁН СЛЕВА, ОТКРЫВАЕТСЯ ТОЛЬКО ОН

Запуск kafka:
```
sudo /opt/kafka/bin/kafka-server-start.sh \ /opt/kafka/config/kraft/server.properties
```
ClickHouse:
```
clickhouse-client --password user
```
Prometheus:
http://localhost:9090/
prometheus:prometheus

postgres:

cd Desktop
./run-postgres.sh
	проверка статуса:
	sudo systemctl status postgresql
	подключение к бд:
	создание базы данных:
	PGPASSWORD=postgres psql -U postgres -h localhost -f ~/data_analitick_3_11/moduls/1_A/dwh_silver_postgres.sql
	psql -h localhost -U postgres -d silver
	список таблиц:
	\dt
	структура таблицы:
	\d silver_detections
	количество записей:
	SELECT COUNT(*) FROM silver_detections;
	последние 5 строк:
	SELECT * FROM silver_detections ORDER BY event_time DESC LIMIT 5;
	выход:
	\q
USERNAME=postgres
PASSWORD=postgres
localhost:5432

SPARK:
```
pyspark
```

Anaconda:
conda info

metabase:
в терминале ./Desktop/metabase/run-metabase.sh
http://localhost:3000

~ start-all.sh

airflow:
```
airflow standalone
```
пароли и юзер в /home/user/airflow/simple_auth_manager_passwords.json.generated 


export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH


rm -rf /tmp/spark/checkpoint_incidents*

pip install -r requirements.txt
