import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
import json

# Connecting to mongodb
client = MongoClient("mongodb://localhost:27017/")
sales_db = client["sales_db"]

# Aggregations
### Customer revenue

sales_db.command({
    "create": "customer_revenue_agg",
    "viewOn": "customers",
    "pipeline": [
        {
            "$lookup": {
                "from": "orders",
                "localField": "customer_id",
                "foreignField": "customer_id",
                "as": "orders"
                }
        }, 
        {"$unwind": "$orders"},
        {"$unwind": "$orders.items"},
        {
            "$lookup": {
                "from": "products",
                "localField": "orders.items.product_id",
                "foreignField": "product_id",
                "as": "products"
            }
        },
        {"$unwind": "$products"},
        {
            "$group": {
                "_id": "$customer_id",
                "totalRevenue": {
                    "$sum": { "$multiply": ["$orders.items.quantity", "$products.price"]}
                }
            }
        },
        {
            "$sort": { "totalRevenue":-1 }
        }
    ]
})

### Product revenue

sales_db.command({
    "create": "product_revenue_agg",
    "viewOn": "products",
    "pipeline": [
        {
            "$lookup": {
                "from": "orders",
                "localField": "product_id",
                "foreignField": "items.product_id",
                "as": "products"
            }
        },
        {"$unwind": "$products"},
        {"$unwind": "$products.items"},
        {
            "$group": {
                "_id": "$product_id",
                "totalRevenue": {
                    "$sum": { "$multiply": ["$products.items.quantity", "$price"]}
                }
            }
        },
        {
            "$sort": { "totalRevenue":-1 }
        }
    ]
})

### Monthly sales

sales_db.command({
    "create": "monthly_sales_agg",
    "viewOn": "orders",
    "pipeline": [
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "orders"
        }},
        {"$unwind": "$orders"},
        {
            "$set": {
                "order_date": { "$toDate": "$order_date"}
            }
        },
        {
            "$group": {
                "_id": {"$month": "$order_date"},
                "sales": {
                    "$sum": { "$multiply": ["$items.quantity", "$orders.price"]}
                }
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
})

# writing data to postgres
### collections

engine = create_engine("postgresql://admin:admin@localhost:5432/warehouse")

# Loading dim_customers table

customers = sales_db["customers"].find()
df_cusotmers = pd.DataFrame(list(customers)).drop(columns= "_id")
df_cusotmers.to_sql("dim_customers", engine, if_exists= "append", index= False)

# Loading dim_products table

products = sales_db["products"].find()
df_products = pd.DataFrame(list(products)).drop(columns= "_id")
df_products.to_sql("dim_products", engine, if_exists= 'append', index= False)

# Loading fact_sales table

pipeline = [
    {"$unwind": "$items"},
    {
        "$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "product"
        }
    },
    {"$unwind": "$product"},
    {
        "$project": {
            "_id": 0,
            "order_id": 1,
            "customer_id": 1,
            "product_id": "$items.product_id",
            "quantity": "$items.quantity",
            "total_amount": {"$multiply": ["$items.quantity", "$product.price"]},
            "order_date": 1
        }
    }
]
fact_sales = sales_db["orders"].aggregate(pipeline=pipeline)
df_orders = pd.DataFrame(list(fact_sales))
print(df_orders)
df_orders.to_sql("fact_sales", engine, if_exists= 'append', index= False)

# Generate reports

customer_revenue = sales_db["customer_revenue_agg"].find()
df_customer_revenue = pd.DataFrame(list(customer_revenue))
df_customer_revenue.to_csv("./exports/customer_revenue.csv")

monthly_sales = sales_db["monthly_sales_agg"].find()
monthly_sales_list = list(monthly_sales)
with open("./exports/monthly_sales.json", "w") as file:
    json.dump(monthly_sales_list, file, indent=4)


