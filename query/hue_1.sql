CREATE TABLE if not exists pizzas (
    id INT,
    name STRING,
    description STRING,
    price DECIMAL(5,2),
    category STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://namenode:8020/user/u5/pizzas.csv'
INTO TABLE u5.pizzas;

/* OR

LOAD DATA LOCAL INPATH '/mnt/pizzas/pizzas.csv'
INTO TABLE u12.pizzas;
*/

SELECT * FROM pizzas;

CREATE TABLE ingredients (
    id INT,
    name STRING,
    unit_price DECIMAL(5,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://namenode:8020/user/u5/ingredients.csv'
INTO TABLE u5.ingredients;

CREATE TABLE customers (
    id INT,
    first_name STRING,
    last_name STRING,
    address STRING,
    phone_number STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://namenode:8020/user/u5/customers.csv'
INTO TABLE u5.customers;

CREATE TABLE orders (
    id INT,
    customer_id INT,
    order_date TIMESTAMP,
    total DECIMAL(7,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://namenode:8020/user/u5/orders.csv'
INTO TABLE u5.orders;

CREATE TABLE order_items (
    order_item_id INT,
    order_id INT,
    pizza_id INT,
    quantity INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://namenode:8020/user/u5/order_items.csv'
INTO TABLE u5.order_items;

CREATE TABLE pizza_ingredients (
    pizza_id INT,
    ingredient_id INT,
    quantity INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://namenode:8020/user/u5/pizza_ingredients.csv'
INTO TABLE u5.pizza_ingredients;

--Найти среднюю стоимость заказа:
SELECT AVG(total) AS average_order_price
FROM orders;

--Найти количество заказов за каждый месяц:
SELECT MONTH(order_date) AS order_month, COUNT(*) AS order_count
FROM orders
GROUP BY MONTH(order_date);

--Найти 5 самых популярных пицц (по количеству заказов):
SELECT p.name, COUNT(oi.pizza_id) AS order_count
FROM order_items oi
JOIN pizzas p ON oi.pizza_id = p.id
GROUP BY p.name
ORDER BY order_count DESC
LIMIT 5;

--Найти клиентов, потративших более 50 долларов на заказы:
SELECT c.first_name, c.last_name, SUM(o.total) AS total_spent
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY c.first_name, c.last_name
HAVING SUM(o.total) > 50;

--Вывести список всех заказов с именами клиентов и названиями заказанных пицц:
SELECT o.id AS order_id, c.first_name, c.last_name, p.name AS pizza_name, oi.quantity
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON o.id = oi.order_id
JOIN pizzas p ON oi.pizza_id = p.id;

--Вывести список пицц с указанием всех ингредиентов, входящих в их состав:
SELECT p.name AS pizza_name, pi.quantity, i.name AS ingredient_name
FROM pizzas p
JOIN pizza_ingredients pi ON p.id = pi.pizza_id
JOIN ingredients i ON pi.ingredient_id = i.id;

=============================================
/* Работа с транзакционными таблицами
В транзакционную таблицу можно писать только insert. Фактически там файлы хранятся иначе, только orc.
И в ней не только 3 столбца ( + служебные поля дополнительные).
Это фактически журнал */

CREATE TABLE IF NOT EXISTS ingredients_inventory (
    ingredient_id INT,
    name STRING,
    quantity INT
)
STORED AS ORC -- Рекомендуемый формат для транзакционных таблиц
TBLPROPERTIES ("transactional"="true");

INSERT INTO ingredients_inventory SELECT id, name, 5 FROM ingredients;

Select * FROM ingredients_inventory

-- Пример операции UPDATE
UPDATE ingredients_inventory SET quantity = quantity + 10 WHERE ingredient_id = 1;

-- Пример операции DELETE
DELETE FROM ingredients_inventory WHERE quantity <= 0;


=================================================
-- Работа с внешними таблицами

CREATE EXTERNAL TABLE IF NOT EXISTS web_logs (
    time_stamp TIMESTAMP,
    ip_address STRING,
    url STRING,
    response_code INT,
    response_size BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/logs/webserver/access_logs';

Select * FROM web_logs;
