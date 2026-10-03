# %% PARAMETERS - toggle this as the parameter cell in Synapse Studio
ROOT = ""

# %%
from pyspark.sql import functions as F

ROOT = ROOT.strip().rstrip("/")
if not ROOT.startswith("abfss://"):
    raise ValueError("Set ROOT to the ADLS landing container abfss:// URL")

def table(layer, name):
    return spark.read.format("delta").load(f"{ROOT}/{layer}/{name}")

names = ("city_food_demand", "restaurant_performance", "city_cuisine_pricing")
failures = []
for name in names:
    count = table("gold", name).count()
    print(f"Gold {name}: {count:,} rows")
    if count == 0:
        failures.append(f"{name} is empty")

silver_items = table("silver", "order_items")
demand = table("gold", "city_food_demand")
silver_units = silver_items.agg(F.sum("quantity").alias("total")).first()["total"] or 0
gold_units = demand.agg(F.sum("units_sold").alias("total")).first()["total"] or 0
print(f"Silver units={silver_units:,.0f}; Gold demand units={gold_units:,.0f}")
if abs(silver_units - gold_units) > 0.001:
    failures.append("Silver units and Gold city demand units differ")

if failures:
    raise ValueError("GOLD DATA QUALITY FAILED: " + "; ".join(failures))
print("GOLD DATA QUALITY PASSED")
