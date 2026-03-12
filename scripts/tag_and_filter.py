from datetime import datetime, UTC
import os
import json
import re
import hashlib

# ----------------------------------
# SNAPSHOT CONFIG
# ----------------------------------

SNAPSHOT_DIR = "snapshots"
LATEST_FILE = "data/latest.json"

# ----------------------------------
# COUNTRY DETECTION
# ----------------------------------

COUNTRY_PATTERNS = {
    "Argentina": [r"\bargentina\b"],
    "Bolivia": [r"\bbolivia\b"],
    "Brazil": [r"\bbrazil\b", r"\bbrasil\b"],
    "Chile": [r"\bchile\b"],
    "Colombia": [r"\bcolombia\b"],
    "Ecuador": [r"\becuador\b"],
    "Guyana": [r"\bguyana\b"],
    "Paraguay": [r"\bparaguay\b"],
    "Peru": [r"\bperu\b", r"\bperú\b"],
    "Uruguay": [r"\buruguay\b"],
    "Venezuela": [r"\bvenezuela\b"],
    "Belize": [r"\bbelize\b"],
    "Costa Rica": [r"\bcosta rica\b"],
    "El Salvador": [r"\bel salvador\b"],
    "Guatemala": [r"\bguatemala\b"],
    "Honduras": [r"\bhonduras\b"],
    "Nicaragua": [r"\bnicaragua\b"],
    "Panama": [r"\bpanama\b", r"\bpanamá\b"],
    "Mexico": [r"\bmexico\b", r"\bm[eé]xico\b"],
}

TARGET_COUNTRIES = set(COUNTRY_PATTERNS.keys())


def detect_country(text):
    if not text:
        return None

    text = text.lower()

    for country, patterns in COUNTRY_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return country
    return None


# ----------------------------------
# JSON EXTRACTION
# ----------------------------------

def extract_embedded_json(text):

    if not text:
        return None

    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1:
        return None

    json_str = text[start:end+1]

    json_str = json_str.replace("“", '"')
    json_str = json_str.replace("”", '"')
    json_str = json_str.replace("’", "'")

    try:
        return json.loads(json_str)
    except:
        return None


# ----------------------------------
# SOURCE DETECTION + NORMALIZATION
# ----------------------------------

def normalize_source(source):

    if not source:
        return None

    source = source.strip().lower()

    source = re.sub(r"https?://", "", source)
    source = source.replace("[.]", ".")

    source = source.split("/")[0]
    source = source.split(":")[0]

    parts = source.split(".")

    if len(parts) >= 2:
        return parts[-2]

    return source


def detect_source(parsed_json):

    if not parsed_json:
        return None

    raw_source = parsed_json.get("Source")

    if not raw_source:
        return None

    return normalize_source(raw_source)

# ----------------------------------
# AUTHOR DETECTION
# ----------------------------------

def detect_author(parsed_json):

    if not parsed_json:
        return None

    author = parsed_json.get("author")

    if not author:
        return None

    author = author.strip()
    author = author.strip("()")

    return author.lower()

# ----------------------------------
# DEDUPLICATION
# ----------------------------------

def content_hash_from_field(content_value):
    normalized = content_value.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def fallback_hash(text):
    normalized = text.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# ----------------------------------
# SECTOR TAGGING
# ----------------------------------

def classify_sector(parsed_json, raw_text):

    if parsed_json:
        content = parsed_json.get("Content", "")
        victim = parsed_json.get("Victim", "")
        msg_type = parsed_json.get("Type", "")
        text = (content + " " + victim + " " + msg_type).lower()
    else:
        text = raw_text.lower()

    if any(word in text for word in [
        "ramson", "ransomware", "ransom alert",
        "published victim", "leak site"
    ]):
        return "Ransomware"

    if any(word in text for word in [
        "ministry", "ministerio", "suprema",
        "judicial", "policia", "asamblea",
        "electoral", ".gov", "state", ".gob",
        "military", "army", "airforce", ".entidad"
    ]):
        return "Government"

    if any(word in text for word in [
        "hospital", "health", "clinic",
        "unimed", "pacientes", "healthcare"
    ]):
        return "Healthcare"

    if any(word in text for word in [
        "bank", "banco", "credit card", "cc ",
        "cvv", "carding", "dump", "track 1",
        "track 2", "cashout", "cash out",
        "paypal", "apple pay", "chime", "ebt",
        "usdt", "crypto", "btc", "eth", "bbva"
        "wallet", "kyc",
    ]):
        return "Financial"

    if any(word in text for word in [
        "combo", "email:pass", "hotmail",
        "mail access", "combolist", "access",
        "combo list", "mailpass", "logs", "stealer",
        "infostealer", "rdp", "browser data"
    ]):
        return "Credential Marketplace"

    if any(word in text for word in [
        "database", "db leak", "dumped database",
        "leaked database", "data breach"
    ]):
        return "Database Leak"

    return "Other"

# ----------------------------------
# MAIN PROCESSING
# ----------------------------------

def main():

    with open("raw_messages.json", "r", encoding="utf-8") as f:
        messages = json.load(f)

    try:
        with open("seen_incidents.json", "r", encoding="utf-8") as f:
            seen_global_keys = set(json.load(f))
    except FileNotFoundError:
        seen_global_keys = set()

    unique_messages = []
    filtered = []
    seen_hashes = set()

    print(f"Original total: {len(messages)}")

    for msg in messages:

        text = msg.get("text", "")

        parsed_json = extract_embedded_json(text)

        # -----------------------------
        # Extract structured fields
        # -----------------------------

        content = None
        victim = None

        if parsed_json:
            content = parsed_json.get("Content")
            victim = parsed_json.get("Victim")

        if not content:
            content = text.split("\n")[0]

        # -----------------------------
        # Deduplication
        # -----------------------------

        if parsed_json and "Content" in parsed_json:
            hash_value = content_hash_from_field(parsed_json["Content"])
        else:
            hash_value = fallback_hash(text)

        if hash_value in seen_hashes:
            continue

        if hash_value in seen_global_keys:
            continue

        seen_hashes.add(hash_value)

        country = detect_country(text)
        sector = classify_sector(parsed_json, text)
        source = detect_source(parsed_json)
        author = detect_author(parsed_json)

        msg["country"] = country
        msg["sector"] = sector
        msg["source"] = source
        msg["author"] = author
        msg["content"] = content
        msg["victim"] = victim
        msg["dedup_key"] = hash_value

        unique_messages.append(msg)

        if country in TARGET_COUNTRIES:
            filtered.append(msg)

#----------------------------------
# SAVE DATASETS
# ----------------------------------
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    os.makedirs("data", exist_ok=True)
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)

    with open("messages_deduplicated.json", "w", encoding="utf-8") as f: json.dump(unique_messages, f, ensure_ascii=False, indent=2)

    with open("messages_filtered.json", "w", encoding="utf-8") as f: json.dump(filtered, f, ensure_ascii=False, indent=2)

    with open(LATEST_FILE, "w", encoding="utf-8") as f: json.dump(filtered, f, ensure_ascii=False, indent=2)

    snapshot_path = os.path.join(SNAPSHOT_DIR, f"{today}.json")

    with open(snapshot_path, "w", encoding="utf-8") as f: json.dump(filtered, f, ensure_ascii=False, indent=2)

    seen_global_keys.update(seen_hashes)

    with open("seen_incidents.json", "w", encoding="utf-8") as f: json.dump(list(seen_global_keys), f, indent=2)

    print(f"Unique after deduplication: {len(unique_messages)}")
    print(f"LATAM filtered: {len(filtered)}")
    print(f"Snapshot saved: {snapshot_path}")

if __name__ == "__main__":
    main()
