# %% PARAMETERS - toggle this as the parameter cell in Synapse Studio
ROOT = ""

# %%
from pyspark.sql import functions as F

ROOT = ROOT.strip().rstrip("/")
if not ROOT.startswith("abfss://"):
    raise ValueError("Set ROOT to the ADLS landing container abfss:// URL")

def table(layer, name):
    return spark.read.format("delta").load(f"{ROOT}/{layer}/{name}")

keys = {
    "food": "f_id", "restaurant": "restaurant_id", "menu": "menu_id",
    "users": "user_id", "order_items": "order_item_id", "reviews": "review_id",
}
failures = []
for name, key in keys.items():
    bronze = table("bronze", name)
    silver = table("silver", name)
    bronze_count, silver_count = bronze.count(), silver.count()
    print(f"{name}: Bronze={bronze_count:,}, Silver={silver_count:,}")
    if bronze_count == 0 or silver_count == 0:
        failures.append(f"{name}: empty Bronze or Silver table")
    if silver_count > bronze_count:
        failures.append(f"{name}: Silver has more rows than Bronze")
    if silver.filter(F.col(key).isNull()).limit(1).count():
        failures.append(f"{name}: null {key}")
    if (silver.groupBy(key).count().filter(F.col("count") > 1)
            .limit(1).count()):
        failures.append(f"{name}: duplicate {key}")

items = table("silver", "order_items")
if items.filter(
    F.col("order_id").isNull() | F.col("restaurant_id").isNull()
    | F.col("f_id").isNull() | F.col("quantity").isNull()
    | (F.col("quantity") <= 0) | F.col("line_amount").isNull()
    | (F.col("line_amount") < 0)
).limit(1).count():
    failures.append("order_items: invalid keys, quantity, or line_amount")

reviews = table("silver", "reviews")
if reviews.filter(~F.col("review_rating").between(1, 5)).limit(1).count():
    failures.append("reviews: rating outside 1-5")

if failures:
    raise ValueError("SILVER DATA QUALITY FAILED: " + "; ".join(failures))
print("SILVER DATA QUALITY PASSED; Gold may run")
