from datetime import datetime, UTC
import json
import os
import re
import hashlib
from pathlib import Path
from collections import Counter, defaultdict

# ----------------------------------
# PATH CONFIG
# ----------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SNAPSHOT_DIR = BASE_DIR.parent / "leaks-data" / "snapshots"
OUTPUT_DIR = BASE_DIR.parent / "leaks-data" / "reports"

RAW_FILE = DATA_DIR / "raw_messages.json"
RUN_STATE_FILE = "data/run_state.json"

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
        "military", "army", "airforce", ".entidad",
        "superintendencia", "nacional", "impuestos",
        "tax", "impuesto", "unidad nacional"
    ]):
        return "Government"

    if any(word in text for word in [
        "hospital", "health", "clinic",
        "unimed", "pacientes", "healthcare",
        "salud", "eps", "ips", "hospitales",
        "clinica"
    ]):
        return "Healthcare"

    if any(word in text for word in [
        "bank", "banco", "credit card", "cc ",
        "cvv", "carding", "dump", "track 1",
        "track 2", "cashout", "cash out",
        "paypal", "apple pay", "chime", "ebt",
        "usdt", "crypto", "btc", "eth", "bbva",
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
# RUN STATE
# ----------------------------------

def load_last_run():
    if not os.path.exists(RUN_STATE_FILE):
        return None
    try:
        with open(RUN_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("last_run")
    except:
        return None

def save_current_run():
    now = datetime.now(UTC).isoformat()
    os.makedirs("data", exist_ok=True)
    with open(RUN_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_run": now}, f, indent=2)
    return now

# ----------------------------------
# SNAPSHOT
# ----------------------------------

def load_previous_snapshot():
    if not os.path.exists(SNAPSHOT_DIR):
        return None
    files = sorted(os.listdir(SNAPSHOT_DIR))
    if len(files) < 2:
        return None
    with open(os.path.join(SNAPSHOT_DIR, files[-2]), "r", encoding="utf-8") as f:
        return json.load(f)

def load_latest_snapshot():
    if not os.path.exists(SNAPSHOT_DIR):
        return []
    files = sorted(os.listdir(SNAPSHOT_DIR))
    if not files:
        return []
    with open(os.path.join(SNAPSHOT_DIR, files[-1]), "r", encoding="utf-8") as f:
        return json.load(f)
# ----------------------------------
# MATRIX + SANKEY
# ----------------------------------

def build_matrix(messages, key_a, key_b):
    matrix = defaultdict(lambda: defaultdict(int))
    for msg in messages:
        a = msg.get(key_a)
        b = msg.get(key_b)
        if not a or not b:
            continue
        matrix[a][b] += 1
    return matrix

def build_sankey(matrix):
    lines = []
    for left, rights in matrix.items():
        for right, count in rights.items():
            lines.append(f"  {left},{right},{count}")
    return "\n".join(lines)

# ----------------------------------
# OVERVIEW
# ----------------------------------

def build_weekly_overview(total, country_counts, sector_counts, source_counts):

    top_countries = [c for c, _ in country_counts.most_common(2)]
    countries_text = " y ".join(top_countries) if top_countries else "N/D"

    sector_parts = []
    for sector, count in sector_counts.most_common(3):
        sector_parts.append(f"{sector} ({count})")

    sectors_text = ", ".join(sector_parts)

    top_sources = [s for s, _ in source_counts.most_common(3)]
    sources_text = ", ".join(top_sources)

    other_explanation = ""
    if "Other" in sector_counts:
        other_explanation = (
            "En esta clasificación, la categoría Other reúne publicaciones que no pudieron asociarse claramente a un sector específico. Estas entradas suelen incluir referencias generales a filtraciones, discusiones en foros o listados de datos cuya naturaleza no es posible identificar con precisión a partir de la información disponible."
        )

    overview = f"""
Este reporte resume referencias a filtraciones observadas en foros, mercados y feeds de monitoreo del ecosistema de filtraciones.

Durante este periodo se identificaron **{total} filtraciones** vinculadas a **{len(country_counts)} países**. **{countries_text}** concentran la mayor parte de los registros observados.

Los sectores más frecuentes corresponden a **{sectors_text}**. {other_explanation}

Varias de estas publicaciones aparecen en plataformas como **{sources_text}**, donde suelen circular este tipo de referencias a bases de datos o listados de credenciales.
"""

    return overview.strip()


def build_delta_section(current_messages, previous_messages):

    if not previous_messages:
        return None

    current_authors = set(m.get("author") for m in current_messages if m.get("author"))
    previous_authors = set(m.get("author") for m in previous_messages if m.get("author"))

    new_authors = current_authors - previous_authors

    current_countries = set(m.get("country") for m in current_messages if m.get("country"))
    previous_countries = set(m.get("country") for m in previous_messages if m.get("country"))

    new_countries = current_countries - previous_countries

    lines = []

    if new_authors:
        lines.append("**Nuevos autores observados:**")
        for a in sorted(new_authors):
            lines.append(f"- {a}")

    if new_countries:
        lines.append("\n**Países observados por primera vez:**")
        for c in sorted(new_countries):
            lines.append(f"- {c}")

    if not lines:
        return "No se observaron cambios significativos respecto al reporte anterior."

    return "\n".join(lines)


def calculate_date_range(messages):

    dates = []

    for m in messages:
        d = m.get("date")
        if not d:
            continue

        try:
            dates.append(datetime.fromisoformat(d))
        except:
            continue

    if not dates:
        return None, None

    start = min(dates)
    end = max(dates)

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def detect_country_increase(current_messages, previous_messages):

    if not previous_messages:
        return None

    current_counts = Counter(
        m["country"] for m in current_messages if m.get("country")
    )

    previous_counts = Counter(
        m["country"] for m in previous_messages if m.get("country")
    )

    changes = {}

    for country, count in current_counts.items():

        prev = previous_counts.get(country, 0)
        diff = count - prev

        if diff > 0:
            changes[country] = diff

    if not changes:
        return None

    biggest_country = max(changes, key=changes.get)
    biggest_value = changes[biggest_country]

    return biggest_country, biggest_value

# ----------------------------------
# MAIN PROCESSING
# ----------------------------------

def main():

    with open(RAW_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    seen = set()
    processed = []

    for msg in raw:

        text = msg.get("text", "")
        parsed = extract_embedded_json(text)

        if parsed and parsed.get("Content"):
            h = content_hash_from_field(parsed["Content"])
        else:
            h = fallback_hash(text)

        if h in seen:
            continue

        seen.add(h)

        msg["content"] = parsed.get("Content") if parsed else text
        msg["country"] = detect_country(text)
        msg["sector"] = classify_sector(parsed, text)
        msg["source"] = detect_source(parsed)
        msg["author"] = detect_author(parsed)
        msg["dedup_key"] = h

        processed.append(msg)

    filtered = [m for m in processed if m.get("country")]

    previous_snapshot = load_latest_snapshot()

    prev_keys = set(m.get("dedup_key") for m in previous_snapshot)
    messages = [m for m in filtered if m["dedup_key"] not in prev_keys]

    delta_section = build_delta_section(messages, previous_snapshot)
    country_increase = detect_country_increase(messages, previous_snapshot)

    last_run = load_last_run()
    current_run = save_current_run()

    if last_run:
        start_date = datetime.fromisoformat(last_run).strftime("%Y-%m-%d")
    else:
        start_date = "Primera ejecución"

    end_date = datetime.fromisoformat(current_run).strftime("%Y-%m-%d")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    filepath = os.path.join(OUTPUT_DIR, f"{today}-filtraciones-latam.md")

    country_counts = Counter(m["country"] for m in messages if m.get("country"))
    sector_counts = Counter(m["sector"] for m in messages if m.get("sector"))
    source_counts = Counter(m["source"] for m in messages if m.get("source"))

    total_incidents = len(messages)

    sector_country = build_matrix(messages, "country", "sector")
    source_country = build_matrix(messages, "source", "country")
    actor_country = build_matrix(messages, "author", "country")

    overview = build_weekly_overview(
        total_incidents,
        country_counts,
        sector_counts,
        source_counts
    )

    sector_country_graph = build_sankey(sector_country)
    source_country_graph = build_sankey(source_country)
    actor_country_graph = build_sankey(actor_country)

    with open(filepath, "w", encoding="utf-8") as f:

        f.write(f"""---
layout: post
title: "Filtraciones LATAM – {start_date} a {end_date}"
datatable: true
---

# Filtraciones LATAM – {start_date} a {end_date}"

**Cobertura de datos:** {start_date} → {end_date}

""")

        f.write("## 🧭 Reporte de filtraciones\n\n")
        f.write(overview + "\n\n")

        if country_increase:
            country, value = country_increase

            f.write("### Señal destacada\n\n")
            f.write(
                f"El país con mayor aumento de actividad en este periodo fue **{country}**, "
                f"con **{value} incidentes adicionales** respecto al snapshot anterior.\n\n"
            )

        if delta_section:
            f.write("## Cambios desde el reporte anterior\n\n")
            f.write(delta_section + "\n\n")

        f.write("## Distribución por país\n\n")

        f.write("```mermaid\n")
        f.write("pie title Países\n")

        for country, count in country_counts.items():
            f.write(f'  "{country}" : {count}\n')

        f.write("```\n\n")

        f.write("## Distribución por sector\n\n")

        f.write("```mermaid\n")
        f.write("pie title Sectores\n")

        for sector, count in sector_counts.items():
            f.write(f'  "{sector}" : {count}\n')

        f.write("```\n\n")

        f.write("## Sector → País\n\n")

        f.write("```mermaid\n")
        f.write("sankey-beta\n")
        f.write(sector_country_graph + "\n")
        f.write("```\n\n")

        f.write("## Origen → País\n\n")

        f.write("```mermaid\n")
        f.write("sankey-beta\n")
        f.write(source_country_graph + "\n")
        f.write("```\n\n")

        f.write("## Autor → País mencionado\n\n")

        f.write("```mermaid\n")
        f.write("sankey-beta\n")
        f.write(actor_country_graph + "\n")
        f.write("```\n\n")

        f.write("## Registro de incidentes\n\n")

        f.write('<table id="incidentTable" class="display compact">\n')
        f.write("<thead>\n<tr>\n")
        f.write("<th>Fecha</th>\n")
        f.write("<th>País</th>\n")
        f.write("<th>Sector</th>\n")
        f.write("<th>Origen</th>\n")
        f.write("<th>Autor</th>\n")
        f.write("<th>Contenido</th>\n")
        f.write("</tr>\n</thead>\n<tbody>\n")

        for m in messages:

            date = m.get("date", "")[:10]
            sector = m.get("sector", "Unknown")
            country = m.get("country", "Unknown")
            author = m.get("author", "-")
            source = m.get("source", "-")
            content = m.get("content", "")


            f.write("<tr>")
            f.write(f"<td>{date}</td>")
            f.write(f"<td>{country}</td>")
            f.write(f"<td>{sector}</td>")
            f.write(f"<td>{source}</td>")
            f.write(f"<td>{author}</td>")
            f.write(f"<td>{content}</td>")
            f.write("</tr>\n")

        f.write("</tbody></table>\n")

    print(f"Reporte generado: {filepath}")

    now = datetime.now(UTC).strftime("%Y-%m-%d_%H-%M")
    snapshot_path = os.path.join(SNAPSHOT_DIR, f"{now}.json")

    combined = previous_snapshot + messages

    seen = set()
    deduped = []

    for m in combined:
        if m["dedup_key"] in seen:
            continue
        seen.add(m["dedup_key"])
        deduped.append(m)

    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2)

if __name__ == "__main__":
    main()
