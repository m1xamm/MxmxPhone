import json

MARKET_FILE = "market.json"

def load_market():
    try:
        with open(MARKET_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def save_market(lots):
    with open(MARKET_FILE, "w", encoding="utf-8") as f:
        json.dump(lots, f, ensure_ascii=False, indent=2)

def add_market_lot(lot):
    lots = load_market()
    lots.append(lot)
    save_market(lots)

def remove_market_lot(lot_id):
    lots = load_market()
    lots = [lot for lot in lots if lot["id"] != lot_id]
    save_market(lots)

def get_market_lot(lot_id):
    for lot in load_market():
        if lot["id"] == lot_id:
            return lot
    return None
