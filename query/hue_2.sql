-- Денормализованное хранение
CREATE TABLE IF NOT EXISTS pizzas2 (
    id INT,
    name STRING,
    ingredients ARRAY<STRING>,  -- Массив ингредиентов
    ratings MAP<STRING, INT>   -- Карта рейтингов от разных пользователей (имя пользователя -> рейтинг)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ';' -- Разделитель элементов массива
MAP KEYS TERMINATED BY ':'        -- Разделитель ключей и значений в карте
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/u5/pizzas2.csv' INTO TABLE pizzas2;

select count(*)
from pizzas2;

-- Выведем данные в виде таблицы "ing"
-- Вывести список всех пицц и их ингредиентов.
select name,  col -- OR ing.*
from pizzas2 lateral view explode(ingredients) ing;

--Найти все пиццы, содержащие "Пепперони" в списке ингредиентов.
SELECT name
FROM pizzas2
WHERE array_contains(ingredients, 'Пепперони');

select name, size(ingredients)
from pizzas2;

select name, ratings
from pizzas2;

--Вывести список всех пицц и их ингредиентов.
SELECT
    p.name AS pizza_name,
    user_rating.key AS user_name,
    user_rating.value AS rating
FROM
    pizzas2 p LATERAL VIEW EXPLODE(p.ratings) user_rating;

-- Найти все пиццы, которые получили рейтинг 5 от пользователя "Мария".
SELECT name
FROM pizzas2
WHERE ratings['Мария'] = 5;

-- Подсчет количества ингредиентов в каждой пицце:
SELECT name, size(ingredients) AS num_ingredients
FROM pizzas2;

-- Подсчет количества ингредиентов в каждой пицце:
SELECT name, size(ingredients) AS num_ingredients
FROM pizzas2;

-- Получение массива ключей (пользователей) из карты рейтингов:
SELECT name, map_keys(ratings) AS users
FROM pizzas2;

-- Получение массива значений (рейтингов) из карты рейтингов:
SELECT name, map_values(ratings) AS ratings_values
FROM pizzas2;

select name, array_agg(map_values(ratings) as users
from pizzas2

-- Оконные функции
-- Для каждого заказа (таблица orders) вывести его ID, дату, цену и сумму продаж нарастающим итогом при сортировке по дате.
SELECT id, order_date, total,
       SUM(total) OVER (ORDER BY order_date) AS running_total
FROM orders;

-- Для каждого заказа вывести его ID, цену и среднюю стоимость текущего и двух предыдущих заказов (в порядке дат).
SELECT id, total,
       AVG(total) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_average
FROM orders;

--Для каждого заказа вывести его ID, ID клиента, цену и ранг цены заказа среди заказов этого клиента (от самого дорогого к самому дешевому).
SELECT id, customer_id, total,
       RANK() OVER (PARTITION BY customer_id ORDER BY total DESC) AS price_rank
FROM orders;
