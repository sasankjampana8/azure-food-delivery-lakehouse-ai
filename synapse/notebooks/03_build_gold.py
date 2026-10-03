# %% PARAMETERS - toggle this as the parameter cell in Synapse Studio
ROOT = ""

# %%
from pyspark.sql import functions as F

ROOT = ROOT.strip().rstrip("/")
if not ROOT.startswith("abfss://"):
    raise ValueError("Set ROOT to the ADLS landing container abfss:// URL")

def silver(name):
    return spark.read.format("delta").load(f"{ROOT}/silver/{name}")

def write_gold(df, name):
    path = f"{ROOT}/gold/{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").save(path))
    print(f"Wrote {path}")

food = silver("food")
restaurant = silver("restaurant")
menu = silver("menu")
items = silver("order_items")
reviews = silver("reviews")

# Food demand at city and item grain.
items_with_context = (items.alias("oi")
    .join(restaurant.alias("r"), F.col("oi.restaurant_id") == F.col("r.restaurant_id"), "left")
    .join(food.alias("f"), F.col("oi.f_id") == F.col("f.f_id"), "left")
    .select(
        F.col("oi.order_id").alias("order_id"),
        F.col("oi.restaurant_id").alias("restaurant_id"),
        F.col("oi.f_id").alias("f_id"),
        F.col("oi.quantity").alias("quantity"),
        F.col("oi.line_amount").alias("line_amount"),
        F.coalesce(F.col("r.city"), F.lit("Unknown")).alias("city"),
        F.coalesce(F.col("f.item"), F.lit("Unknown")).alias("item"),
        F.col("f.veg_or_non_veg").alias("veg_or_non_veg")))

gold_demand = (items_with_context
    .groupBy("city", "f_id", "item", "veg_or_non_veg")
    .agg(F.countDistinct("order_id").alias("orders_containing_item"),
         F.sum("quantity").alias("units_sold"),
         F.round(F.sum("line_amount"), 2).alias("item_sales_amount")))
write_gold(gold_demand, "city_food_demand")

# Restaurant performance; the item order count is distinct within each restaurant.
item_metrics = (items.groupBy("restaurant_id")
    .agg(F.countDistinct("order_id").alias("orders_with_items"),
         F.sum("quantity").alias("units_sold"),
         F.round(F.sum("line_amount"), 2).alias("item_sales_amount")))
review_metrics = (reviews.groupBy("restaurant_id")
    .agg(F.count("*").alias("review_count"),
         F.round(F.avg("review_rating"), 2).alias("average_review_rating")))

gold_restaurant = (restaurant.alias("r")
    .join(item_metrics.alias("i"), "restaurant_id", "left")
    .join(review_metrics.alias("v"), "restaurant_id", "left")
    .select("restaurant_id", "restaurant_name", "city", "cuisine",
            "restaurant_rating", "cost_for_two",
            F.coalesce(F.col("orders_with_items"), F.lit(0)).alias("orders_with_items"),
            F.coalesce(F.col("units_sold"), F.lit(0)).alias("units_sold"),
            F.coalesce(F.col("item_sales_amount"), F.lit(0.0)).alias("item_sales_amount"),
            F.coalesce(F.col("review_count"), F.lit(0)).alias("review_count"),
            "average_review_rating"))
write_gold(gold_restaurant, "restaurant_performance")

# Menu pricing at city and cuisine grain.
menu_with_restaurant = (menu.alias("m")
    .join(restaurant.alias("r"), F.col("m.restaurant_id") == F.col("r.restaurant_id"), "left")
    .select(
        F.col("m.menu_id").alias("menu_id"),
        F.col("m.restaurant_id").alias("restaurant_id"),
        F.col("m.menu_price").alias("menu_price"),
        F.coalesce(F.col("r.city"), F.lit("Unknown")).alias("city"),
        F.coalesce(F.col("m.menu_cuisine"), F.col("r.cuisine"), F.lit("Unknown")).alias("cuisine"))
    .filter(F.col("menu_price").isNotNull()))

gold_pricing = (menu_with_restaurant.groupBy("city", "cuisine")
    .agg(F.countDistinct("restaurant_id").alias("restaurant_count"),
         F.count("menu_id").alias("menu_items"),
         F.round(F.avg("menu_price"), 2).alias("average_menu_price"),
         F.round(F.min("menu_price"), 2).alias("minimum_menu_price"),
         F.round(F.max("menu_price"), 2).alias("maximum_menu_price")))
write_gold(gold_pricing, "city_cuisine_pricing")
print("GOLD COMPLETE")
