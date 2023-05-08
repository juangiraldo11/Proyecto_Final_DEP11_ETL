from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
transform = Transform()
load = Load()

df_customers = extract.read_cloud_storage("datalake_proyecto_final","landing/customers")
df_orders = extract.read_cloud_storage("datalake_proyecto_final", "landing/orders") 
df_order_items = extract.read_cloud_storage("datalake_proyecto_final", "landing/order_items") 
df_departments = extract.read_cloud_storage("datalake_proyecto_final", "landing/departments") 
df_categories = extract.read_cloud_storage("datalake_proyecto_final","landing/categories") 
df_products = extract.read_cloud_storage("datalake_proyecto_final","landing/products") 

df_enunciado1 = transform.enunciado1(df_customers, df_orders, df_order_items)
df_enunciado2 = transform.enunciado2(df_order_items, df_products,df_categories)
df_enunciado3 = transform.enunciado3(df_customers, df_orders, df_order_items, df_products, df_categories)
df_enunciado4 = transform.enunciado4(df_customers, df_orders, df_order_items, df_products, df_categories)
df_mi_enunciado = transform.mi_enunciado(df_customers, df_orders, df_order_items, df_products, df_categories)

load.load_to_cloud_storage(df_enunciado1,"datalake_proyecto_final","gold/df_enunciado1")                     load.load_to_cloud_storage(df_enunciado2,"datalake_proyecto_final","gold/df_enunciado2")
load.load_to_cloud_storage(df_enunciado3,"datalake_proyecto_final","gold/df_enunciado3")
load.load_to_cloud_storage(df_enunciado4,"datalake_proyecto_final","gold/df_enunciado4")
load.load_to_cloud_storage(df_mi_enunciado,"datalake_proyecto_final","gold/df_mi_enunciado")                                        