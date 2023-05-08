
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, locals()) 

class Transform():
    
    def __init__(self) -> None:
        self.process = 'Transform Process'


    def enunciado1(self, customer, orders, items ):
        q = """
                SELECT
                    customer_id, customer_fname, customer_lname, customer_email, sum(order_item_quantity) as quantity_item_total, sum(order_item_subtotal)as total
                FROM
                    customer as c
                INNER JOIN
                    orders as o
                    ON c.customer_id = o.order_customer_id
                INNER JOIN
                    items as oi
                    ON o.order_id = oi.order_item_order_id
                WHERE order_status <> 'CANCELED'
                GROUP BY customer_id, customer_fname, customer_lname, customer_email
                ORDER BY  total DESC
                LIMIT 20
            """
        result = sqldf(q)

        return result
        

    def enunciado2(self, df_order_items,df_products,df_categories):
        q = """
                SELECT
                    ca.category_name, sum(order_item_quantity) as item_quantity, cast(sum(order_item_subtotal) AS INT )as total
                FROM df_order_items as oi
                INNER JOIN
                    df_products as p
                    ON oi.order_item_product_id = p.product_id
                INNER JOIN
                    df_categories as ca
                    ON p.product_category_id = ca.category_id
                GROUP BY ca.category_name
            """

        result = sqldf(q)

        return result

    def enunciado3(self,df_customer,df_orders,df_order_items,df_products, df_categories):
        q = """SELECT
                customer_city, category_name
                FROM (SELECT
                    customer_city, category_name, count(category_name) as quantity, DENSE_RANK () OVER ( 
                                PARTITION BY customer_city 
                                ORDER BY count(category_name) DESC
                            ) rank
                    FROM
                        df_customer as c
                    INNER JOIN
                        df_orders as o
                        ON c.customer_id = o.order_customer_id
                    INNER JOIN
                        df_order_items as oi
                        ON o.order_id = oi.order_item_order_id
                    INNER JOIN
                        df_products as p
                        ON oi.order_item_product_id = p.product_id
                    INNER JOIN
                        df_categories as ca
                        ON p.product_category_id = ca.category_id
                    GROUP BY customer_city, category_name
                    ) t
            WHERE rank = 1
            """
        result = sqldf(q)
        
        return result


    def enunciado4(self,df_customer,df_orders,df_order_items,df_products, df_categories):
        q = """
            SELECT
                    customer_city, product_name, quantity, total
                    FROM (SELECT
                        customer_city, product_name,sum(order_item_quantity) as quantity,sum(order_item_subtotal) as total, DENSE_RANK () OVER ( 
                                    PARTITION BY customer_city 
                                    ORDER BY sum(order_item_quantity) DESC
                                ) rank
                        FROM
                            df_customer as c
                        INNER JOIN
                            df_orders as o
                            ON c.customer_id = o.order_customer_id
                        INNER JOIN
                            df_order_items as oi
                            ON o.order_id = oi.order_item_order_id
                        INNER JOIN
                            df_products as p
                            ON oi.order_item_product_id = p.product_id
                        INNER JOIN
                            df_categories as ca
                            ON p.product_category_id = ca.category_id
                        GROUP BY customer_city, category_name
                        ) t
                WHERE rank < 6
                ORDER BY quantity DESC
            """
        result = sqldf(q)
        
        return result


    def mi_enunciado(self,df_customer,df_orders,df_order_items,df_products, df_categories):
         
        q = """
            SELECT
                    customer_city, category_name
                FROM
                    df_customer AS c
                INNER JOIN
                    df_orders AS o
                    ON c.customer_id = o.order_customer_id
                INNER JOIN
                    df_order_items AS oi
                    ON o.order_id = oi.order_item_order_id
                INNER JOIN
                    df_products AS p
                    ON oi.order_item_product_id = p.product_id
                INNER JOIN
                    df_categories AS ca
                    ON p.product_category_id = ca.category_id
                GROUP BY
                    customer_city, category_name
                HAVING
                    COUNT(category_name) = (
                        SELECT
                            COUNT(category_name)
                        FROM
                            df_customer AS c2
                        INNER JOIN
                            df_orders AS o2
                            ON c2.customer_id = o2.order_customer_id
                        INNER JOIN
                            df_order_items AS oi2
                            ON o2.order_id = oi2.order_item_order_id
                        INNER JOIN
                            df_products AS p2
                            ON oi2.order_item_product_id = p2.product_id
                        INNER JOIN
                            df_categories AS ca2
                            ON p2.product_category_id = ca2.category_id
                        WHERE
                            c.customer_city = c2.customer_city
                        GROUP BY
                            customer_city, category_name
                        ORDER BY
                            COUNT(category_name) DESC
                        LIMIT 1
                    )
                """
        result = sqldf(q)
            
        return result

