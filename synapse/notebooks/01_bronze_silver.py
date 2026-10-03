# %% PARAMETERS - toggle this as the parameter cell in Synapse Studio
ROOT = ""  # ADF base parameter, e.g. abfss://landing@<account>.dfs.core.windows.net

# %%
import re
import uuid
from pyspark.sql import functions as F

ROOT = ROOT.strip().rstrip("/")
if not ROOT.startswith("abfss://"):
    raise ValueError("Set ROOT to the ADLS landing container abfss:// URL")

spark.conf.set("spark.sql.session.timeZone", "UTC")
run_id = str(uuid.uuid4())

# A small contract: the six expected landing files and required source columns.
SOURCE_FILES = {
    "food": "food/food.csv",
    "menu": "menu/menu.csv",
    "order_items": "order_items/order_items.csv",
    "restaurant": "restaurant/restaurant.csv",
    "reviews": "reviews/reviews.csv",
    "users": "users/users.csv",
}
REQUIRED = {
    "food": {"f_id", "item", "veg_or_non_veg"},
    "menu": {"menu_id", "r_id", "f_id", "cuisine", "price"},
    "order_items": {"order_item_id", "order_id", "r_id", "f_id", "price", "quantity", "line_amount"},
    "restaurant": {"id", "name", "city", "rating", "cuisine", "cost"},
    "reviews": {"review_id", "order_id", "user_id", "restaurant_id", "rating", "comment", "review_date"},
    "users": {"user_id", "name", "email"},
}


def normalized(df):
    names = [re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", c.strip().lower())).strip("_") for c in df.columns]
    if any(not name for name in names) or len(names) != len(set(names)):
        raise ValueError(f"Invalid or colliding normalized headers: {names}")
    df = df.toDF(*names)
    for column in ("c0", "unnamed_0"):
        if column in df.columns:
            df = df.drop(column)
    return df


def cleaned_string(name):
    value = F.trim(F.col(name))
    return F.when(value == "", F.lit(None)).otherwise(value)


def numeric(name):
    value = F.regexp_replace(F.col(name), r"[^0-9.\-]", "")
    return F.when(value == "", F.lit(None)).otherwise(value.cast("double"))


def write_delta(df, layer, name):
    path = f"{ROOT}/{layer}/{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").save(path))
    print(f"Wrote {path}")


def read_delta(layer, name):
    return spark.read.format("delta").load(f"{ROOT}/{layer}/{name}")


# Preflight every file before overwriting any Bronze table.
raw = {}
for name, relative_path in SOURCE_FILES.items():
    df = normalized(spark.read.option("header", "true").option("inferSchema", "false")
                    .csv(f"{ROOT}/{relative_path}"))
    missing = REQUIRED[name] - set(df.columns)
    if missing:
        raise ValueError(f"{relative_path} missing columns: {sorted(missing)}")
    if df.limit(1).count() == 0:
        raise ValueError(f"{relative_path} is empty")
    raw[name] = df
    print(f"Checked {relative_path}: {len(df.columns)} columns")

# Bronze preserves source values as strings and records ingestion lineage.
for name, df in raw.items():
    bronze = (df.withColumn("_run_id", F.lit(run_id))
               .withColumn("_ingested_at_utc", F.current_timestamp())
               .withColumn("_source_file", F.lit(SOURCE_FILES[name])))
    write_delta(bronze, "bronze", name)

# Silver reads persisted Bronze Delta, rather than the landing CSV DataFrames.
bronze = {name: read_delta("bronze", name) for name in SOURCE_FILES}

silver_food = (bronze["food"].select(
    cleaned_string("f_id").alias("f_id"),
    cleaned_string("item").alias("item"),
    cleaned_string("veg_or_non_veg").alias("veg_or_non_veg"))
    .filter(F.col("f_id").isNotNull()).dropDuplicates(["f_id"]))

silver_restaurant = (bronze["restaurant"].select(
    cleaned_string("id").alias("restaurant_id"),
    cleaned_string("name").alias("restaurant_name"),
    cleaned_string("city").alias("city"),
    cleaned_string("cuisine").alias("cuisine"),
    numeric("rating").alias("restaurant_rating"),
    numeric("cost").alias("cost_for_two"))
    .filter(F.col("restaurant_id").isNotNull()).dropDuplicates(["restaurant_id"]))

silver_menu = (bronze["menu"].select(
    cleaned_string("menu_id").alias("menu_id"),
    cleaned_string("r_id").alias("restaurant_id"),
    cleaned_string("f_id").alias("f_id"),
    cleaned_string("cuisine").alias("menu_cuisine"),
    numeric("price").alias("menu_price"))
    .filter(F.col("menu_id").isNotNull() & F.col("restaurant_id").isNotNull()
            & F.col("f_id").isNotNull()).dropDuplicates(["menu_id"]))

# Keep source passwords out of Silver and Gold.
silver_users = (bronze["users"].select(
    cleaned_string("user_id").alias("user_id"),
    cleaned_string("name").alias("user_name"),
    F.lower(cleaned_string("email")).alias("email"))
    .filter(F.col("user_id").isNotNull()).dropDuplicates(["user_id"]))

silver_order_items = (bronze["order_items"].select(
    cleaned_string("order_item_id").alias("order_item_id"),
    cleaned_string("order_id").alias("order_id"),
    cleaned_string("r_id").alias("restaurant_id"),
    cleaned_string("f_id").alias("f_id"),
    numeric("price").alias("unit_price"),
    numeric("quantity").alias("quantity"),
    numeric("line_amount").alias("line_amount"))
    .filter(F.col("order_item_id").isNotNull() & F.col("order_id").isNotNull()
            & F.col("restaurant_id").isNotNull() & F.col("f_id").isNotNull()
            & (F.col("quantity") > 0) & (F.col("line_amount") >= 0))
    .dropDuplicates(["order_item_id"]))

silver_reviews = (bronze["reviews"].select(
    cleaned_string("review_id").alias("review_id"),
    cleaned_string("order_id").alias("order_id"),
    cleaned_string("user_id").alias("user_id"),
    cleaned_string("restaurant_id").alias("restaurant_id"),
    numeric("rating").alias("review_rating"),
    cleaned_string("comment").alias("comment"),
    F.to_date(F.col("review_date")).alias("review_date"))
    .filter(F.col("review_id").isNotNull() & F.col("restaurant_id").isNotNull()
            & F.col("review_rating").between(1, 5))
    .dropDuplicates(["review_id"]))

silver_tables = {
    "food": silver_food, "restaurant": silver_restaurant, "menu": silver_menu,
    "users": silver_users, "order_items": silver_order_items, "reviews": silver_reviews,
}
for name, df in silver_tables.items():
    write_delta(df, "silver", name)

print(f"Bronze and Silver complete; run_id={run_id}")
