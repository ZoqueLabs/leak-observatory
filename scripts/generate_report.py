import json
from collections import Counter, defaultdict
from datetime import datetime, UTC
import os
import re

INPUT_FILE = "data/latest.json"
SNAPSHOT_DIR = "../leaks-data/snapshots"
OUTPUT_DIR = "../leaks-data/reports"

# -----------------------------
# Run state
# -----------------------------

RUN_STATE_FILE = "data/run_state.json"


def load_last_run():

    if not os.path.exists(RUN_STATE_FILE):
        return None

    try:
        with open(RUN_STATE_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()

            if not content:
                return None

            data = json.loads(content)
            return data.get("last_run")

    except Exception:
        return None


def save_current_run():

    now = datetime.now(UTC).isoformat()

    os.makedirs("data", exist_ok=True)

    with open(RUN_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_run": now}, f, indent=2)

    return now


# -----------------------------
# Snapshot helper
# -----------------------------

def load_previous_snapshot():

    if not os.path.exists(SNAPSHOT_DIR):
        return None

    files = sorted(os.listdir(SNAPSHOT_DIR))

    if len(files) < 2:
        return None

    previous_file = os.path.join(SNAPSHOT_DIR, files[-2])

    with open(previous_file, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------
# Helpers
# -----------------------------

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


# -----------------------------
# Main
# -----------------------------

def main():

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        messages = json.load(f)

    last_run = load_last_run()
    current_run = save_current_run()

    if last_run:
        start_date = datetime.fromisoformat(last_run).strftime("%Y-%m-%d")
    else:
        start_date = "Primera ejecución"

    end_date = datetime.fromisoformat(current_run).strftime("%Y-%m-%d")

    previous_messages = load_previous_snapshot()
    delta_section = build_delta_section(messages, previous_messages)
    country_increase = detect_country_increase(messages, previous_messages)

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


if __name__ == "__main__":
    main()
