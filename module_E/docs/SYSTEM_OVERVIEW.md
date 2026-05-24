# Обзор системы

## Цель

Платформа мониторинга дорожной обстановки: детекция из видео → метрики и опасные события → batch и прогноз → дашборд для Департамента транспорта.

## Модули

- **А** — DWH Medallion, Kafka, диаграммы draw.io
- **Б** — YOLOv8, MinIO, Kafka
- **В** — потоковые метрики и инциденты, DQ-логи
- **Г** — batch 30 мин, Airflow, ELT
- **Д** — Prophet, Metabase
- **Е** — документация и презентация

## Стек

MinIO · PostgreSQL · ClickHouse · Kafka · Airflow · Metabase · YOLOv8

## Репозиторий

`data_analitick_3_11_v3` — каждый модуль с собственным `README.md`.
