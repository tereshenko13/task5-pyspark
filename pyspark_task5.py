import findspark
findspark.init()
import mysql.connector
import pandas as pd
from pyspark.sql import SparkSession

appName = "PySpark MySQL Example"
master = "local"

spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

conn =mysql.connector.connect(host="localhost", 
                              user="root", 
                              passwd="12345678", 
                              auth_plugin="mysql_native_password", 
                              database="sakila")
cursor = conn.cursor()
#Вывести количество фильмов в каждой категории, отсортировать по убыванию
query1 = "SELECT category_id, COUNT(film_id) as count FROM film_category GROUP BY category_id ORDER BY count DESC"
pdf1 = pd.read_sql(query1, con=conn)

df1 = spark.createDataFrame(pdf1)

df1.show()

#Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
query2="SELECT actor.first_name, actor.last_name, count(rental.rental_id) as sum FROM actor, film_actor, inventory, rental WHERE actor.actor_id=film_actor.actor_id and film_actor.film_id=inventory.film_id and inventory.inventory_id=rental.inventory_id group by actor.first_name, actor.last_name order by sum desc limit 10"
pdf2 = pd.read_sql(query2, con=conn)

df2 = spark.createDataFrame(pdf2)

df2.show()

#Вывести категорию фильмов, на которую потратили больше всего денег.
query3="SELECT category.name, sum(payment.amount) as sum FROM category, film_category, inventory, rental, payment WHERE category.category_id=film_category.category_id and film_category.film_id=inventory.film_id and inventory.inventory_id=rental.inventory_id and rental.rental_id=payment.rental_id group by category.name order by sum desc limit 1"
pdf3 = pd.read_sql(query3, con=conn)

df3 = spark.createDataFrame(pdf3)

df3.show()

#Вывести названия фильмов, которых нет в inventory.
query4="SELECT film.title FROM film LEFT JOIN inventory ON inventory.film_id = film.film_id WHERE inventory.film_id IS NULL"
pdf4 = pd.read_sql(query4, con=conn)

df4 = spark.createDataFrame(pdf4)

df4.show()

#Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
query5="SELECT t.sum, t.first_name, t.last_name From (SELECT count(film_category.category_id) as sum,dense_rank() OVER (order by count(film_category.category_id) DESC) as rnk, actor.first_name, actor.last_name FROM film_category, category, actor, film_actor where actor.actor_id=film_actor.actor_id and film_actor.film_id=film_category.film_id and film_category.category_id=category.category_id and category.name='Children' group by actor.first_name, actor.last_name  order by sum desc) t where t.rnk in (1,2,3)"
pdf5 = pd.read_sql(query5, con=conn)

df5 = spark.createDataFrame(pdf5)

df5.show()

#Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
query6="SELECT city.city, count(customer.active='1') as activecount, count(customer.active='0') as unactivecount FROM city, address, customer WHERE city.city_id=address.city_id and address.address_id=customer.address_id group by city.city order by unactivecount desc"
pdf6 = pd.read_sql(query6, con=conn)

df6 = spark.createDataFrame(pdf6)

df6.show()

#Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. Тоже самое сделать для городов в которых есть символ “-”.
query7="SELECT category.name, SUM(TIMESTAMPDIFF(HOUR,rental.rental_date,rental.return_date)) as vremya FROM  address, city, customer, rental, inventory, film_category, category where city.city_id=address.city_id and address.address_id=customer.address_id and customer.customer_id=rental.customer_id  and rental.inventory_id=inventory.inventory_id and inventory.film_id=film_category.film_id and film_category.category_id=category.category_id and city.city LIKE 'A%' and city.city like '%-%' group by category.name order by vremya desc limit 1"
pdf7 = pd.read_sql(query7, con=conn)

df7 = spark.createDataFrame(pdf7)

df7.show()

conn.commit()
conn.close()