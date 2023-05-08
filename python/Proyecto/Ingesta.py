from process.Extract import Extract
from process.Load import Load

extract = Extract()
load = Load ()

print ( "Extracting Data")

print("Customers")
df_customers=extract.read_mysql("retail_db","customers")

print("Orders")
df_orders=extract.read_mysql("retail_db","orders")

print("Order Items")
df_order_items=extract.read_mysql("retail_db","order_items")

print("Categories")
df_categories=extract.read_cloud_storage("proyecto-final-dep11","retail/categories")

print("Products")
df_products=extract.read_cloud_storage("proyecto-final-dep11","retail/products")

print("Departments")
df_departments=extract.read_cloud_storage("proyecto-final-dep11","retail/departments")

print ( "Extraction Done")

print ("#####################################")

print ("Charging Data to Landing")
load.load_to_cloud_storage(df_customers,"datalake_proyecto_final","landing/customers")
load.load_to_cloud_storage(df_orders,"datalake_proyecto_final","landing/orders")
load.load_to_cloud_storage(df_order_items,"datalake_proyecto_final","landing/order_items")
load.load_to_cloud_storage(df_categories,"datalake_proyecto_final","landing/categories")
load.load_to_cloud_storage(df_products,"datalake_proyecto_final","landing/products")
load.load_to_cloud_storage(df_departments,"datalake_proyecto_final","landing/departments")

