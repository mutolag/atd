#!/usr/bin/env python3
"""
Интерактивная очистка или удаление структур данных (Kafka, PostgreSQL, MinIO, ClickHouse).
Поддерживает два режима:
  - очистка данных (оставляет таблицы/бакеты/топики, но удаляет содержимое)
  - удаление структур (полное удаление таблиц/бакетов/топиков)
Запустите скрипт без аргументов и следуйте подсказкам.
"""

import logging
import sys
import time
from typing import List, Tuple

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import boto3
from botocore.exceptions import ClientError
import psycopg2
from psycopg2 import sql
from clickhouse_driver import Client as ClickHouseClient

# ========== Конфигурация ==========
CONFIG = {
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topics": ["raw_detections", "anomaly_log"],
        "num_partitions": 1,
        "replication_factor": 1,
        "enabled": True,
    },
    "postgresql": {
        "host": "localhost",
        "port": 5432,
        "dbname": "silver",
        "user": "postgres",
        "password": "postgres",
        "tables": ["silver_detections"],
        "enabled": True,
    },
    "minio": {
        "endpoint": "http://localhost:9001",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket": "bronze-frames",
        "enabled": True,
    },
    "clickhouse": {
        "host": "localhost",
        "port": 9000,
        "user": "default",
        "password": "user",
        "database": "transport",
        "tables": [
            "streaming_metrics",
            "gold_traffic_aggregates",
            "gold_predictions",
            "gold_incidents",
            "gold_data_quality_audit",
            "gold_watermark",
        ],
        "enabled": True,
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def ask_user_choice(components: List[Tuple[str, str]]) -> List[str]:
    """Интерактивный выбор компонентов."""
    print("\nДоступные компоненты:")
    for i, (key, desc) in enumerate(components, 1):
        print(f"  {i}. {desc}")
    print("  a. Выбрать ВСЁ")
    print("  q. Выйти без действий")

    while True:
        choice = input("\nВведите номера через пробел (например: 1 3) или 'a' или 'q': ").strip()
        if choice.lower() == 'q':
            print("Выход.")
            sys.exit(0)
        if choice.lower() == 'a':
            return [key for key, _ in components]

        selected = []
        parts = choice.split()
        valid = True
        for p in parts:
            if p.isdigit():
                idx = int(p)
                if 1 <= idx <= len(components):
                    selected.append(components[idx - 1][0])
                else:
                    print(f"Неверный номер: {idx}. Попробуйте снова.")
                    valid = False
                    break
            else:
                print(f"Неверный ввод: {p}. Введите номера через пробел.")
                valid = False
                break
        if valid and selected:
            return selected


def ask_operation() -> str:
    """Выбор операции: очистка (c) или удаление структур (d)."""
    print("\nВыберите операцию:")
    print("  c - очистить данные (TRUNCATE / удалить объекты / пересоздать топики)")
    print("  d - удалить структуры (DROP TABLE / DELETE BUCKET / удалить топики без пересоздания)")
    while True:
        op = input("Ваш выбор (c/d, по умолчанию c): ").strip().lower()
        if op in ('c', ''):
            return 'clean'
        if op == 'd':
            return 'drop'
        print("Пожалуйста, введите 'c' или 'd'.")


# ========== Функции очистки данных (clean) ==========

def clean_kafka(dry_run: bool = False) -> bool:
    cfg = CONFIG["kafka"]
    topics = cfg["topics"]
    servers = cfg["bootstrap_servers"]
    logger.info("=== Очистка Kafka: топики %s ===", topics)
    if dry_run:
        logger.info("[dry-run] Будут удалены и заново созданы топики: %s", topics)
        return True
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
    except Exception as e:
        logger.error("Не удалось подключиться к Kafka: %s", e)
        return False
    try:
        for topic in topics:
            try:
                admin.delete_topics([topic])
                logger.info("Топик '%s' удалён.", topic)
                time.sleep(0.5)
            except UnknownTopicOrPartitionError:
                logger.info("Топик '%s' не существовал.", topic)
            except Exception as e:
                logger.error("Ошибка при удалении топика '%s': %s", topic, e)
                return False
        new_topics = [
            NewTopic(
                name=topic,
                num_partitions=cfg["num_partitions"],
                replication_factor=cfg["replication_factor"],
            )
            for topic in topics
        ]
        admin.create_topics(new_topics)
        logger.info("Топики созданы заново: %s", topics)
        return True
    except TopicAlreadyExistsError:
        logger.info("Один из топиков уже существует, но это не проблема.")
        return True
    except Exception as e:
        logger.error("Ошибка: %s", e)
        return False
    finally:
        admin.close()


def clean_postgresql(dry_run: bool = False) -> bool:
    cfg = CONFIG["postgresql"]
    tables = cfg["tables"]
    logger.info("=== Очистка PostgreSQL: TRUNCATE таблиц %s ===", tables)
    if dry_run:
        for tbl in tables:
            logger.info("[dry-run] Будет выполнено TRUNCATE TABLE %s", tbl)
        return True
    conn = None
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
            user=cfg["user"], password=cfg["password"]
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.Identifier(table)))
                logger.info("Таблица '%s' очищена.", table)
        return True
    except Exception as e:
        logger.error("Ошибка при очистке PostgreSQL: %s", e)
        return False
    finally:
        if conn:
            conn.close()


def clean_minio(dry_run: bool = False) -> bool:
    cfg = CONFIG["minio"]
    bucket = cfg["bucket"]
    logger.info("=== Очистка MinIO: удаление всех объектов из бакета '%s' ===", bucket)
    if dry_run:
        logger.info("[dry-run] Будут удалены все объекты из бакета %s", bucket)
        return True
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=cfg["endpoint"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
        )
    except Exception as e:
        logger.error("Не удалось создать клиент MinIO: %s", e)
        return False
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info("Бакет '%s' не существует, очистка не требуется.", bucket)
            return True
        else:
            logger.error("Ошибка доступа: %s", e)
            return False
    deleted = 0
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            objects = page.get("Contents", [])
            if objects:
                keys = [{"Key": obj["Key"]} for obj in objects]
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                deleted += len(objects)
        logger.info("Бакет '%s' очищен. Удалено объектов: %d", bucket, deleted)
        return True
    except Exception as e:
        logger.error("Ошибка при очистке MinIO: %s", e)
        return False


def clean_clickhouse(dry_run: bool = False) -> bool:
    cfg = CONFIG["clickhouse"]
    db = cfg["database"]
    tables = cfg["tables"]
    logger.info("=== Очистка ClickHouse: TRUNCATE таблиц в %s ===", db)
    if dry_run:
        for tbl in tables:
            logger.info("[dry-run] Будет TRUNCATE TABLE %s.%s", db, tbl)
        return True
    try:
        ch = ClickHouseClient(
            host=cfg["host"], port=cfg["port"],
            user=cfg["user"], password=cfg["password"], database=db
        )
    except Exception as e:
        logger.error("Не удалось подключиться к ClickHouse: %s", e)
        return False
    ok = True
    for tbl in tables:
        try:
            ch.execute(f"TRUNCATE TABLE {db}.{tbl}")
            logger.info("Таблица %s.%s очищена.", db, tbl)
        except Exception as e:
            if "doesn't exist" in str(e) or "DB::Exception: Table" in str(e):
                logger.warning("Таблица %s.%s не существует, пропускаем.", db, tbl)
            else:
                logger.error("Ошибка очистки %s.%s: %s", db, tbl, e)
                ok = False
    ch.disconnect()
    return ok


# ========== Функции удаления структур (drop) ==========

def drop_kafka(dry_run: bool = False) -> bool:
    cfg = CONFIG["kafka"]
    topics = cfg["topics"]
    servers = cfg["bootstrap_servers"]
    logger.info("=== Удаление топиков Kafka: %s ===", topics)
    if dry_run:
        logger.info("[dry-run] Топики будут удалены без пересоздания: %s", topics)
        return True
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
    except Exception as e:
        logger.error("Не удалось подключиться к Kafka: %s", e)
        return False
    try:
        admin.delete_topics(topics)
        logger.info("Топики удалены: %s", topics)
        return True
    except UnknownTopicOrPartitionError:
        logger.info("Некоторые топики не существовали.")
        return True
    except Exception as e:
        logger.error("Ошибка при удалении топиков: %s", e)
        return False
    finally:
        admin.close()


def drop_postgresql_tables(dry_run: bool = False) -> bool:
    cfg = CONFIG["postgresql"]
    tables = cfg["tables"]
    logger.info("=== Удаление таблиц PostgreSQL: %s ===", tables)
    if dry_run:
        for tbl in tables:
            logger.info("[dry-run] Будет выполнено DROP TABLE IF EXISTS %s CASCADE", tbl)
        return True
    conn = None
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
            user=cfg["user"], password=cfg["password"]
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table))
                )
                logger.info("Таблица '%s' удалена.", table)
        return True
    except Exception as e:
        logger.error("Ошибка при удалении таблиц PostgreSQL: %s", e)
        return False
    finally:
        if conn:
            conn.close()


def drop_minio_bucket(dry_run: bool = False) -> bool:
    cfg = CONFIG["minio"]
    bucket = cfg["bucket"]
    logger.info("=== Удаление бакета MinIO: '%s' ===", bucket)
    if dry_run:
        logger.info("[dry-run] Бакет %s будет удалён (со всем содержимым)", bucket)
        return True
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=cfg["endpoint"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
        )
    except Exception as e:
        logger.error("Не удалось создать клиент MinIO: %s", e)
        return False
    # Проверяем существование бакета
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info("Бакет '%s' не существует, удаление не требуется.", bucket)
            return True
        else:
            logger.error("Ошибка доступа: %s", e)
            return False
    # Удаляем все объекты, затем бакет
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            objects = page.get("Contents", [])
            if objects:
                keys = [{"Key": obj["Key"]} for obj in objects]
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        s3.delete_bucket(Bucket=bucket)
        logger.info("Бакет '%s' успешно удалён.", bucket)
        return True
    except ClientError as e:
        logger.error("Ошибка при удалении бакета MinIO: %s", e)
        return False
    except Exception as e:
        logger.error("Непредвиденная ошибка: %s", e)
        return False


def drop_clickhouse_tables(dry_run: bool = False) -> bool:
    cfg = CONFIG["clickhouse"]
    db = cfg["database"]
    tables = cfg["tables"]
    logger.info("=== Удаление таблиц ClickHouse: %s ===", tables)
    if dry_run:
        for tbl in tables:
            logger.info("[dry-run] Будет выполнено DROP TABLE IF EXISTS %s.%s", db, tbl)
        return True
    try:
        ch = ClickHouseClient(
            host=cfg["host"], port=cfg["port"],
            user=cfg["user"], password=cfg["password"], database=db
        )
    except Exception as e:
        logger.error("Не удалось подключиться к ClickHouse: %s", e)
        return False
    ok = True
    for tbl in tables:
        try:
            ch.execute(f"DROP TABLE IF EXISTS {db}.{tbl}")
            logger.info("Таблица %s.%s удалена.", db, tbl)
        except Exception as e:
            logger.error("Ошибка удаления %s.%s: %s", db, tbl, e)
            ok = False
    ch.disconnect()
    return ok


def main():
    components = [
        ("kafka", "Kafka (топики raw_detections, anomaly_log)"),
        ("postgresql", "PostgreSQL (таблица silver_detections)"),
        ("minio", "MinIO (бакет bronze-frames)"),
        ("clickhouse", "ClickHouse (все таблицы transport.*)"),
    ]

    selected_keys = ask_user_choice(components)
    if not selected_keys:
        print("Ничего не выбрано. Выход.")
        sys.exit(0)

    print("\nВыбранные компоненты:")
    for key in selected_keys:
        print(f"  - {key}")

    # Выбор операции: очистка или удаление
    operation = ask_operation()

    # Подтверждение реального выполнения
    dry_run_input = input("\nВыполнить реальное действие? (y/N): ").strip().lower()
    dry_run = dry_run_input != 'y'

    if dry_run:
        logger.info("=== РЕЖИМ DRY-RUN: изменения не будут применены ===")
    else:
        logger.info("=== РЕЖИМ РЕАЛЬНОГО ВЫПОЛНЕНИЯ ===")

    # Словарь функций для каждого компонента и операции
    actions = {
        "kafka": {
            "clean": clean_kafka,
            "drop": drop_kafka,
        },
        "postgresql": {
            "clean": clean_postgresql,
            "drop": drop_postgresql_tables,
        },
        "minio": {
            "clean": clean_minio,
            "drop": drop_minio_bucket,
        },
        "clickhouse": {
            "clean": clean_clickhouse,
            "drop": drop_clickhouse_tables,
        },
    }

    results = {}
    for key in selected_keys:
        func = actions.get(key, {}).get(operation)
        if not func:
            logger.error("Нет реализации для '%s' -> '%s'", key, operation)
            results[key] = False
            continue
        # Имя для отчёта
        name = {"kafka": "Kafka", "postgresql": "PostgreSQL",
                "minio": "MinIO", "clickhouse": "ClickHouse"}.get(key, key)
        results[name] = func(dry_run=dry_run)

    print("\n=== Итоги ===")
    all_ok = True
    for name, ok in results.items():
        status = "✅ ОК" if ok else "❌ ОШИБКА"
        print(f"{name:12}: {status}")
        if not ok:
            all_ok = False

    if all_ok:
        print("\nГотово! Выбранное действие выполнено успешно.")
    else:
        print("\nДействие завершено с ошибками. Проверьте логи.")

if __name__ == "__main__":
    main()