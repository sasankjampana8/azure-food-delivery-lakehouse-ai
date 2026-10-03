"""Offline checks for the checked-in Azure project artifacts."""
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
template = json.loads((ROOT / "adf" / "ARMTemplateForFactory.json").read_text())
assert template["resources"], "Empty ADF template"
resources = template["resources"]
pipelines = {
    resource["name"].split("'/")[-1].split("'")[0]: resource["properties"]
    for resource in resources if resource["type"].endswith("/pipelines")
}
assert set(pipelines) == {"pl_ingest_all_files", "pl_ingest_food"}, pipelines.keys()
activities = pipelines["pl_ingest_all_files"]["activities"]
names = [activity["name"] for activity in activities]
assert names == ["ForEach1", "TransformFoodDelivery", "ValidateFoodDelivery", "BuildGold", "ValidateGold"], names
expected = ["nb_01_bronze_silver", "nb_02_validate_silver", "nb_03_build_gold", "nb_04_validate_gold"]
for i, notebook in enumerate(expected, 1):
    activity = activities[i]
    assert activity["type"] == "SynapseNotebook"
    assert activity["typeProperties"]["notebook"]["referenceName"]["value"] == notebook
    assert activity["dependsOn"] == [{"activity": names[i - 1], "dependencyConditions": ["Succeeded"]}]
    assert activity["typeProperties"]["parameters"]["ROOT"]["value"].startswith("abfss://")
    path = ROOT / "synapse" / "notebooks" / f"{notebook}.ipynb"
    data = json.loads(path.read_text())
    first = data["cells"][0]
    assert first["cell_type"] == "code" and "ROOT =" in "".join(first["source"])
    assert "parameters" in first.get("metadata", {}).get("tags", [])
    for cell in data["cells"]:
        if cell["cell_type"] == "code":
            ast.parse("".join(cell["source"]), filename=str(path))
ast.parse((ROOT / "app" / "app.py").read_text(), filename="app/app.py")
print("ADF chain, parameter cells, and Python syntax: OK")
