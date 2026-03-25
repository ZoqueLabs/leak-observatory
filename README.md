# Exfiltradaz - Pipeline
Exfiltradaz es una iniciativa para recolectar, estructurar y visibilizar información sobre filtraciones y exposición de datos en América Latina a partir de fuentes abiertas.

Este repositorio contiene el pipeline encargado de la recolección y procesamiento de datos.

Este proyecto extrae mensajes desde canales de Telegram, identifica información relevante, normaliza los datos y genera snapshots y reportes por periodo.

## Qué hace este pipeline
El pipeline implementa un flujo de procesamiento que transforma mensajes crudos en datos estructurados:

* Recolección de mensajes desde Telegram
* Extracción de datos desde contenido embebido
* Filtrado de incidentes relacionados con LATAM
* Normalización de campos (país, sector, fuente, etc.)
* Generación de snapshots (JSON)
* Generación de reportes (Markdown) a partir de los snapshots

Este repositorio contiene el proceso.
Los datos generados se encuentran publicados en el repo:

https://github.com/ZoqueLabs/leaks-data

## Estructura del proyecto
```
.
├── scripts/      # Scripts del pipeline
├── snapshots/    # Salida temporal (JSON)
├── reports/      # Salida temporal (Markdown)
├── data/         # Datos locales no versionados
├── README.md
└── .gitignore
```

## Scripts principales
* `connect_telegram.py` Conecta con la API de Telegram y permite acceder a los canales configurados
* `incremental_snapshot.py` Recolecta nuevos mensajes desde la última ejecución
* `tag_and_filter.py` Extrae campos estructurados y filtra incidentes relacionados con LATAM
* `generate_report.py` Genera reportes a partir de los snapshots

## Instalación
Clonar el repositorio:
```
git clone https://github.com/ZoqueLabs/leak-observatory
cd leak-observatory
```

Instalar dependencias:
```
pip install -r requirements.txt
```

## Configuración
Crear un archivo .env en la raíz del proyecto:
```
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
```
Puedes obtener estas credenciales en:

https://my.telegram.org

## Ejecución del pipeline
Recolectar mensajes:
```
python scripts/incremental_snapshot.py
```
Procesar y filtrar:
```
python scripts/tag_and_filter.py
```
Generar reporte:
```
python scripts/generate_report.py
```

## Salida
El pipeline genera:

* `snapshots/YYYY-MM-DD.json` → dataset estructurado por periodo
* `reports/YYYY-MM-DD.md` → reporte generado a partir del snapshot

## Alcance y limitaciones
* Depende de fuentes públicas accesibles
* No todas las filtraciones son detectadas
* La extracción depende de la calidad del contenido original
* La clasificación puede ser parcial o ambigua

## Relación con Exfiltradaz
Este pipeline es el componente técnico de Exfiltradaz.
* Procesa y estructura los datos
* Genera los snapshots y reportes
* Alimenta el repositorio de datos y las publicaciones periódicas
