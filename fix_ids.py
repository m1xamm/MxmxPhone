import json, uuid

with open("data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for uid, user in data.items():
    if isinstance(user, dict) and "inventory" in user:
        for item in user["inventory"]:
            if "id" in item and len(item["id"]) > 12:
                item["id"] = uuid.uuid4().hex[:8]

with open("data.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
