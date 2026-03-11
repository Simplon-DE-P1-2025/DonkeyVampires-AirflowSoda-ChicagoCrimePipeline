# Chicago Crime Pipeline

Pipeline de données automatisé pour ingérer, vérifier, transformer et charger les données de criminalité de Chicago vers PostgreSQL.

## Stack technique

| Outil | Rôle |
|---|---|
| Apache Airflow (Astro CLI) | Orchestration du pipeline |
| Soda Core + DuckDB | Contrôle qualité des données |
| Pandas + PyArrow | Transformation et Parquet I/O |
| SQLAlchemy + psycopg2 | Chargement PostgreSQL |
| Render | Hébergement PostgreSQL |
| Docker | Environnement d'exécution |

## Architecture du pipeline

```
fetch_chicago_crime_data
        │  JSON brut
        ▼
   check_soda (check 1)
        │  contrat qualité sur le JSON
        ▼
transform_chicago_crime
        │  nettoyage → Parquet
        ▼
check_soda_post_transform (check 2)
        │  contrat qualité sur le Parquet
        ▼
    filter_rows
        │  valid_*.parquet  /  invalid_*.parquet (quarantaine)
        ▼
  load_to_postgres
        │  lignes valides uniquement
        ▼
  PostgreSQL (table chicago_crime)
```

## Structure du projet

```
astro_project/
├── dags/
│   └── pipeline_chicago_crime.py   # DAG principal (6 tâches)
├── src/
│   ├── ingestion.py                # Appel API Chicago Data Portal
│   ├── soda_check.py               # Vérification qualité Soda V4
│   ├── transformation.py           # Nettoyage et typage des données
│   ├── filtering.py                # Filtrage valid / invalid
│   └── load.py                     # Chargement PostgreSQL
├── include/
│   ├── soda_scan/
│   │   ├── soda_rules_firstcheck.yml   # Contrat qualité (données brutes)
│   │   └── soda_rules_secondcheck.yml  # Contrat qualité (post-transform)
│   ├── create_tables.sql           # Schéma PostgreSQL
│   └── data/
│       ├── raw/                    # JSON bruts (ingestion)
│       ├── transformed/            # Parquet nettoyés
│       └── filtered/               # Parquet valid + quarantaine
├── requirements.txt
├── Dockerfile
└── airflow_settings.yaml           # Connexion Airflow locale (postgres_chicago_crime)
```

## Fonctionnement du DAG

Le DAG `pipeline_chicago_crime` est défini dans `dags/pipeline_chicago_crime.py`. Il s'exécute manuellement (`schedule=None`) et enchaîne 6 tâches :

| # | Tâche | Fonction appelée | Entrée | Sortie (XCom) |
|---|---|---|---|---|
| 1 | `fetch_chicago_crime_data` | `ingestion.fetch_chicago_crime_data` | API Chicago Data Portal | chemin du JSON brut |
| 2 | `check_soda` | `soda_check.run_check_soda` | JSON brut (XCom tâche 1) | dict résultat Soda |
| 3 | `transform_chicago_crime` | `transformation.run_transform_chicago_crime` | JSON brut (XCom tâche 1) | chemin du Parquet nettoyé |
| 4 | `check_soda_post_transform` | `soda_check.run_check_soda_post_transform` | Parquet (XCom tâche 3) | dict résultat Soda |
| 5 | `filter_rows` | `filtering.run_filter_rows` | Parquet (XCom tâche 3) | `{"valid": "...", "invalid": "..."}` |
| 6 | `load_to_postgres` | `load.run_load` | dict valid/invalid (XCom tâche 5) | `{"valid_rows_loaded": N, "quarantine_path": "..."}` |

**Chaîne de dépendances :**
```
fetch_chicago_crime_data >> check_soda >> transform_chicago_crime
    >> check_soda_post_transform >> filter_rows >> load_to_postgres
```

Chaque tâche récupère le chemin du fichier produit par la tâche précédente via `xcom_pull`. Si un check Soda échoue, le contrat qualité est rompu mais le pipeline continue (comportement configurable).

## Scripts source (`src/`)

### `ingestion.py`
Interroge l'API publique [Chicago Data Portal](https://data.cityofchicago.org) (endpoint Socrata), récupère les données de criminalité en JSON, et les sauvegarde dans `include/data/raw/raw_crimes_<timestamp>.json`.

### `soda_check.py`
Encapsule les vérifications qualité **Soda V4** via DuckDB (moteur in-memory).

- `check_soda(file_path, contract_file_path)` — charge le fichier (JSON ou Parquet détecté automatiquement) dans une table DuckDB `chicago_crime`, exécute le contrat YAML, écrit un rapport dans `include/reports/`, retourne le résultat.
- `run_check_soda(**context)` — wrapper Airflow pour le **check 1** (données brutes, `soda_rules_firstcheck.yml`).
- `run_check_soda_post_transform(**context)` — wrapper Airflow pour le **check 2** (post-transformation, `soda_rules_secondcheck.yml`).

### `transformation.py`
Charge le JSON brut, applique le pipeline de nettoyage suivant, et sauvegarde le résultat en Parquet dans `include/data/transformed/` :

1. Suppression des colonnes calculées (`:@computed_region_*`)
2. Remplissage des coordonnées manquantes depuis le champ `location`
3. Sélection des colonnes utiles uniquement
4. Parsing des dates (`date`, `updated_on`)
5. Typage des colonnes (`id` → int, `year`/`district` → int, `latitude`/`longitude` → float, booléens)
6. Déduplication sur `id`

### `filtering.py`
Applique des règles de validation ligne par ligne sur le Parquet transformé et produit deux fichiers :

- `include/data/filtered/valid_<timestamp>.parquet` — lignes conformes, prêtes pour PostgreSQL
- `include/data/filtered/invalid_<timestamp>.parquet` — lignes rejetées (quarantaine)

Règles appliquées : `id` non nul, `date` et `year` entre 2001 et 2026, `primary_type` dans la liste officielle Chicago PD, `district` entre 1 et 25, coordonnées dans les bornes géographiques de Chicago.

### `load.py`
Charge les lignes valides dans PostgreSQL via SQLAlchemy.

- `get_engine()` — crée le moteur à partir des variables d'environnement.
- `execute_sql_file(engine, path)` — exécute `include/create_tables.sql` (idempotent, `IF NOT EXISTS`).
- `load_parquet(engine, path, table_name)` — lit le Parquet et insère via `df.to_sql()`.
- `run_load(**context)` — wrapper Airflow : crée la table si besoin, charge `valid_*.parquet` dans `chicago_crime`, logue le chemin de quarantaine sans le charger.

## Prérequis

- [Docker](https://docs.docker.com/get-docker/) + [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Compte [Render](https://render.com) avec une base PostgreSQL

## Démarrage rapide

```bash
# 1. Cloner le repo
git clone https://github.com/Simplon-DE-P1-2025/DonkeyVampires-AirflowSoda-ChicagoCrimePipeline.git
cd DonkeyVampires-AirflowSoda-ChicagoCrimePipeline/astro_project

# 2. Créer le fichier .env
cp .env.example .env   # puis renseigner les credentials PostgreSQL

# 3. Démarrer Airflow
astro dev start

# 4. Ouvrir l'interface Airflow
# http://localhost:8080  (admin / admin)
```

## Variables d'environnement

Créer un fichier `.env` dans `astro_project/` :

```env
POSTGRES_HOST=HOST
POSTGRES_PORT=PORT
POSTGRES_DB=DB_NAME
POSTGRES_USER=USER_NAME
POSTGRES_PASSWORD=PASSWORD
```

> ⚠️ Utiliser le **hostname externe** Render (visible dans Dashboard → PostgreSQL → External Database URL). Le hostname interne n'est pas résolvable depuis Docker.

## Contrôles qualité (Soda)

Deux contrats YAML sont exécutés via **Soda V4 + DuckDB** à des étapes différentes du pipeline. Chaque contrat génère un rapport Markdown dans `include/reports/`.

---

### Check 1 — Données brutes (`soda_rules_firstcheck.yml`)

Exécuté sur le **JSON brut** issu de l'API, avant toute transformation.

| Catégorie | Colonne(s) | Contrôles réalisés |
|---|---|---|
| Identifiant | `id` | Non nul, sans doublon, valeur ≥ 1 |
| Identifiant | `case_number` | Non nul |
| Dates | `date`, `updated_on` | Non nul |
| Dates | `year` | Non nul, entre 2001 et 2026 |
| Localisation | `block` | Non nul |
| Codes administratifs | `beat` | Non nul, entre 100 et 2535 |
| Codes administratifs | `district` | Non nul, entre 1 et 25 (districts Chicago PD) |
| Codes administratifs | `ward` | Non nul, entre 1 et 50 (wards de Chicago) |
| Codes administratifs | `community_area` | Non nul, entre 1 et 77 (community areas) |
| Classification | `iucr`, `primary_type`, `description`, `fbi_code` | Non nul |
| Booléens | `arrest`, `domestic` | Non nul |
| Coordonnées projetées | `x_coordinate` | Entre 1 050 000 et 1 210 000 (bornes Chicago) |
| Coordonnées projetées | `y_coordinate` | Entre 1 810 000 et 1 955 000 (bornes Chicago) |
| Coordonnées GPS | `latitude` | Entre 41.60 et 42.05 |
| Coordonnées GPS | `longitude` | Entre -87.95 et -87.50 |

---

### Check 2 — Données transformées (`soda_rules_secondcheck.yml`)

Exécuté sur le **Parquet nettoyé**, après transformation. Les règles sont plus strictes car les données ont été typées et dédupliquées.

| Catégorie | Colonne(s) | Contrôles réalisés |
|---|---|---|
| Identifiant | `id` | Non nul, **sans doublon** (déduplication vérifiée) |
| Identifiant | `case_number` | Non nul |
| Dates | `date` | Non nul, type `timestamp`, entre 2001-01-01 et 2026-12-31 |
| Dates | `year` | Non nul, entre 2001 et 2026 |
| Dates | `updated_on` | Non nul, type `timestamp` |
| Classification | `primary_type` | Non nul, valeur dans les **31 types officiels Chicago PD** |
| Classification | `iucr`, `fbi_code` | Non nul |
| Coordonnées GPS | `latitude` | Entre 41.60 et 42.05 |
| Coordonnées GPS | `longitude` | Entre -87.95 et -87.50 |
| Coordonnées projetées | `x_coordinate` | Entre 1 050 000 et 1 210 000 |
| Coordonnées projetées | `y_coordinate` | Entre 1 810 000 et 1 955 000 |

---

### Filtrage post-check 2

Après le second check, `filtering.py` applique les mêmes règles de manière **ligne par ligne** et produit :

- ✅ `valid_*.parquet` — lignes conformes, chargées dans PostgreSQL
- ❌ `invalid_*.parquet` — lignes rejetées, conservées en **quarantaine Parquet** (non chargées en base)

## Connexion Airflow

La connexion `postgres_chicago_crime` est déclarée dans `airflow_settings.yaml` et chargée automatiquement au démarrage local.