CREATE TABLE IF NOT EXISTS orders2 (
    order_id INT,
    customer_id INT,
    total DECIMAL(10, 2),
    order_month INT,
    order_year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/u5/orders_partitioned.csv' INTO TABLE orders2;

select count(*) from orders2;

--Загрузить данные из таблицы orders2 в партиционированную таблицу orders_partitioned, создавая партиции вручную.
INSERT OVERWRITE TABLE orders_partitioned PARTITION (order_month=1, order_year=2023)
SELECT order_id, customer_id, total
FROM orders2
WHERE order_month = 1 AND order_year = 2023;

ALTER TABLE orders_partitioned ADD PARTITION (order_month=2, order_year=2023);
INSERT OVERWRITE TABLE orders_partitioned PARTITION (order_month=2, order_year=2023)
SELECT order_id, customer_id, total
FROM orders2
WHERE order_month = 2 AND order_year = 2023;

ALTER TABLE orders_partitioned ADD PARTITION (order_month=1, order_year=2024);
INSERT OVERWRITE TABLE orders_partitioned PARTITION (order_month=1, order_year=2024)
SELECT order_id, customer_id, total
FROM orders2
WHERE order_month = 1 AND order_year = 2024;

ALTER TABLE orders_partitioned ADD PARTITION (order_month=2, order_year=2024);
INSERT OVERWRITE TABLE orders_partitioned PARTITION (order_month=2, order_year=2024)
SELECT order_id, customer_id, total
FROM orders2
WHERE order_month = 2 AND order_year = 2024;

-- Восстановление метаданных партиций. Альтернатива ALTER TABLE
MSCK REPAIR TABLE orders_partitioned;

show PARTITIONS orders_partitioned;

-- Найти все заказы за январь 2023 года.
SELECT *
FROM orders_partitioned
WHERE order_month = 1 AND order_year = 2023;
-- Этот запрос будет сканировать только партицию order_month=1 и order_year=2023, что значительно быстрее, чем сканирование всей таблицы.


-- Загрузка с динамическим партиционированием

-- Загрузить все данные из таблицы orders2 в партиционированную таблицу orders_partitioned используя динамическое партиционирование.
-- Сначала нужно установить режим динамического партиционирования:
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict; -- nonstrict позволяет создавать партиции на лету. Корневая партиция по умолчанию должна быть указана.

INSERT OVERWRITE TABLE orders_partitioned_2 PARTITION (order_month, order_year)
SELECT order_id, customer_id, total, order_month, order_year
FROM orders;

select * from orders_partitioned_2;

-- Бакетизация

-- Бакетизация (bucketing) — это техника, которая разбивает данные таблицы на фиксированное количество "бакетов" на основе хеш-функции, примененной к одному или нескольким столбцам.
-- В отличие от партиционирования, количество бакетов задается заранее и не зависит от количества уникальных значений в столбце.
-- Бакетизация полезна для оптимизации запросов, особенно соединений, так как данные с одинаковыми значениями хеш-функции гарантированно находятся в одном и том же бакете.

CREATE TABLE IF NOT EXISTS customers_bucketed (
    customer_id INT,
    customer_name STRING,
    city STRING
)
CLUSTERED BY (customer_id) INTO 4 BUCKETS -- Бакетизация по customer_id на 4 бакета
STORED AS ORC;

CREATE TABLE IF NOT EXISTS orders_bucketed (
    order_id INT,
    customer_id INT,
    order_date DATE,
    total DECIMAL(10, 2)
)
CLUSTERED BY (customer_id) INTO 4 BUCKETS
STORED AS ORC;

SET hive.enforce.bucketing = true;
SET hive.enforce.sorting = true; -- сортировать внутри бакета

INSERT OVERWRITE TABLE customers_bucketed SELECT * FROM customers;
INSERT OVERWRITE TABLE orders_bucketed SELECT * FROM orders;

--Получить список всех заказов с именами клиентов.
SELECT o.order_id, c.customer_name
FROM orders_bucketed o
JOIN customers_bucketed c ON o.customer_id = c.customer_id;

SELECT o.order_id, c.customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
