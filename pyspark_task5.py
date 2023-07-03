import findspark
findspark.init()
import mysql.connector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col,desc, dense_rank, to_timestamp
from pyspark.sql.window import Window



spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
           .getOrCreate()

conn =mysql.connector.connect(host="localhost", 
                              user="root", 
                              passwd="12345678", 
                              auth_plugin="mysql_native_password", 
                              database="sakila")

jdbcUrl="jdbc:mysql://localhost:3306/sakila"

cursor = conn.cursor()

df_actor = spark.read.jdbc(url=jdbcUrl, table="actor",  properties={"user": "root", "password": "12345678"})
df_category = spark.read.jdbc(url=jdbcUrl, table="category",  properties={"user": "root", "password": "12345678"})
df_film_category = spark.read.jdbc(url=jdbcUrl, table="film_category", properties={"user": "root", "password": "12345678"})
df_address = spark.read.jdbc(url=jdbcUrl, table="address", properties={"user": "root", "password": "12345678"})
df_country = spark.read.jdbc(url=jdbcUrl, table="country", properties={"user": "root", "password": "12345678"})
df_customer = spark.read.jdbc(url=jdbcUrl, table="customer", properties={"user": "root", "password": "12345678"})
df_film = spark.read.jdbc(url=jdbcUrl, table="film", properties={"user": "root", "password": "12345678"})
df_film_actor = spark.read.jdbc(url=jdbcUrl, table="film_actor", properties={"user": "root", "password": "12345678"})
df_inventory = spark.read.jdbc(url=jdbcUrl, table="inventory", properties={"user": "root", "password": "12345678"})
df_language = spark.read.jdbc(url=jdbcUrl, table="language", properties={"user": "root", "password": "12345678"})
df_payment = spark.read.jdbc(url=jdbcUrl, table="payment", properties={"user": "root", "password": "12345678"})
df_rental = spark.read.jdbc(url=jdbcUrl, table="rental", properties={"user": "root", "password": "12345678"})
df_staff = spark.read.jdbc(url=jdbcUrl, table="staff", properties={"user": "root", "password": "12345678"})
df_store = spark.read.jdbc(url=jdbcUrl, table="store", properties={"user": "root", "password": "12345678"})
df_city = spark.read.jdbc(url=jdbcUrl, table="city", properties={"user": "root", "password": "12345678"})

#Вывести количество фильмов в каждой категории, отсортировать по убыванию
query1 = "SELECT category_id, COUNT(film_id) as count FROM film_category GROUP BY category_id ORDER BY count DESC"
pdf1 = pd.read_sql(query1, con=conn)

df1 = spark.createDataFrame(pdf1)

df1.show()

query1_pyspark=df_category.join(df_film_category,"category_id") \
    .select(df_category["name"].alias("category_name"), df_film_category["film_id"]) \
    .groupBy("category_name") \
    .agg({"film_id": "count"}) \
    .withColumnRenamed("count(film_id)", "films_count") \
    .orderBy(desc(col("films_count"))).show()

#Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
query2="SELECT actor.first_name, actor.last_name, count(rental.rental_id) as sum FROM actor, film_actor, inventory, rental WHERE actor.actor_id=film_actor.actor_id and film_actor.film_id=inventory.film_id and inventory.inventory_id=rental.inventory_id group by actor.first_name, actor.last_name order by sum desc limit 10"
pdf2 = pd.read_sql(query2, con=conn)

df2 = spark.createDataFrame(pdf2)

df2.show()

query2_pyspark = df_actor.join(df_film_actor, df_actor.actor_id == df_film_actor.actor_id) \
    .join(df_inventory, df_film_actor.film_id == df_inventory.film_id) \
    .join(df_rental, df_inventory.inventory_id == df_rental.inventory_id) \
    .groupBy(df_actor.first_name, df_actor.last_name) \
    .agg(count(df_rental.rental_id).alias("sum")) \
    .orderBy(desc("sum")) \
    .limit(10).show()

#Вывести категорию фильмов, на которую потратили больше всего денег.
query3="SELECT category.name, sum(payment.amount) as sum FROM category, film_category, inventory, rental, payment WHERE category.category_id=film_category.category_id and film_category.film_id=inventory.film_id and inventory.inventory_id=rental.inventory_id and rental.rental_id=payment.rental_id group by category.name order by sum desc limit 1"
pdf3 = pd.read_sql(query3, con=conn)

df3 = spark.createDataFrame(pdf3)

df3.show()

query3_pyspark = (
    df_category.join(df_film_category, df_category.category_id == df_film_category.category_id)
    .join(df_inventory, df_film_category.film_id == df_inventory.film_id)
    .join(df_rental, df_inventory.inventory_id == df_rental.inventory_id)
    .join(df_payment, df_rental.rental_id == df_payment.rental_id)
    .groupBy(df_category.name)
    .agg(sum(df_payment.amount).alias("sum"))
    .orderBy("sum", ascending=False)
    .limit(1)
).show()

#Вывести названия фильмов, которых нет в inventory.
query4="SELECT film.title FROM film LEFT JOIN inventory ON inventory.film_id = film.film_id WHERE inventory.film_id IS NULL"
pdf4 = pd.read_sql(query4, con=conn)

df4 = spark.createDataFrame(pdf4)

df4.show()

query4_pyspark = df_film.join(df_inventory, df_film.film_id == df_inventory.film_id, "left") \
    .where(df_inventory.film_id.isNull()) \
    .select(df_film.title).show()

#Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
query5="SELECT t.sum, t.first_name, t.last_name From (SELECT count(film_category.category_id) as sum,dense_rank() OVER (order by count(film_category.category_id) DESC) as rnk, actor.first_name, actor.last_name FROM film_category, category, actor, film_actor where actor.actor_id=film_actor.actor_id and film_actor.film_id=film_category.film_id and film_category.category_id=category.category_id and category.name='Children' group by actor.first_name, actor.last_name  order by sum desc) t where t.rnk in (1,2,3)"
pdf5 = pd.read_sql(query5, con=conn)

df5 = spark.createDataFrame(pdf5)

df5.show()

query5_pyspark= df_actor.join(df_film_actor, df_actor.actor_id == df_film_actor.actor_id) \
              .join(df_film_category, df_film_actor.film_id == df_film_category.film_id) \
              .join(df_category, df_film_category.category_id == df_category.category_id) \
              .filter(df_category.name == "Children")\
              .groupBy(df_actor.first_name, df_actor.last_name).agg(count(df_category.category_id).alias("sum"))\
              .withColumn("rnk", dense_rank().over(Window.orderBy(desc("sum"))))\
              .select("sum", "first_name", "last_name").filter(col("rnk").isin([1, 2, 3]))\
              .show()


#Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
query6="SELECT city.city, count(customer.active='1') as activecount, count(customer.active='0') as unactivecount FROM city, address, customer WHERE city.city_id=address.city_id and address.address_id=customer.address_id group by city.city order by unactivecount desc"
pdf6 = pd.read_sql(query6, con=conn)

df6 = spark.createDataFrame(pdf6)

df6.show()

query6_pyspark = df_city.join(df_address, df_city.city_id == df_address.city_id) \
                .join(df_customer, df_address.address_id == df_customer.address_id)\
                .groupBy("city") \
                .agg(count((df_customer.active == '1').cast("int")).alias("activecount"),
                         count((df_customer.active == '0').cast("int")).alias("unactivecount"))\
                .orderBy(col("unactivecount").desc(),col("city")).show()

#Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. Тоже самое сделать для городов в которых есть символ “-”.
query7="SELECT category.name, SUM(TIMESTAMPDIFF(HOUR,rental.rental_date,rental.return_date)) as vremya FROM  address, city, customer, rental, inventory, film_category, category where city.city_id=address.city_id and address.address_id=customer.address_id and customer.customer_id=rental.customer_id  and rental.inventory_id=inventory.inventory_id and inventory.film_id=film_category.film_id and film_category.category_id=category.category_id and city.city LIKE 'A%' and city.city like '%-%' group by category.name order by vremya desc limit 1"
pdf7 = pd.read_sql(query7, con=conn)

df7 = spark.createDataFrame(pdf7)

df7.show()

query7_pyspark=df_city.join(df_address,df_city.city_id == df_address.city_id) \
    .join(df_customer, df_address.address_id==df_customer.address_id) \
    .join(df_rental, df_customer.customer_id==df_rental.customer_id) \
    .join(df_inventory, df_rental.inventory_id == df_inventory.inventory_id) \
    .join(df_film_category, df_inventory.film_id == df_film_category.film_id) \
    .join(df_category, df_film_category.category_id == df_category.category_id) \
    .where(col("city").like("A%") & col("city").like("%-%")) \
    .groupBy(col("name")) \
    .agg((sum((to_timestamp(df_rental.return_date).cast("long") - to_timestamp(df_rental.rental_date).cast("long"))/3600)).alias("vremya")) \
    .orderBy(col("vremya").desc()) \
    .limit(1)\
    .select(col("name")).show()

conn.commit()
conn.close()