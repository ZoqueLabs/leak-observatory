# Leak Observatory LATAM

Pipeline to monitor leak announcements from Telegram channels and generate structured reports about data leaks affecting Latin America.

The project collects messages, extracts structured data from embedded JSON, filters incidents related to LATAM countries, and generates periodic analytical reports.

---

## Project structure

scripts/ → pipeline scripts  
snapshots/ → processed incident snapshots  
reports/ → generated reports  
data/ → local working data (not versioned)

Main scripts:

- connect_telegram.py → connects to Telegram and retrieves messages
- incremental_snapshot.py → collects new messages
- tag_and_filter.py → extracts JSON fields and filters LATAM incidents
- generate_report.py → builds the analytical report

---

## Installation

Clone the repository:

git clone https://github.com/...
cd leak-observatory

Install dependencies:

pip install -r requirements.txt

---

## Configuration

Create a `.env` file in the project root:

TELEGRAM_API_ID=your_api_id  
TELEGRAM_API_HASH=your_api_hash  

You can obtain Telegram API credentials from:

https://my.telegram.org

---

## Running the pipeline

1. Collect messages from Telegram

python scripts/incremental_snapshot.py

2. Extract structured data and filter LATAM incidents

python scripts/tag_and_filter.py

3. Generate the analytical report

python scripts/generate_report.py

---

## Output

The pipeline generates:

snapshots/YYYY-MM-DD.json → structured dataset of incidents  
reports/YYYY-MM-DD.md → analytical report

---
