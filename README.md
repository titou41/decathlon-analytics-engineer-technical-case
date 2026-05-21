# Decathlon Domyos — Analytics Engineer Technical Case

Pipeline dbt pour l'analyse de l'expérimentation commerciale **retrait du kit haltères 10kg** (Domyos, 2023).

---

## Contexte

En S35-S41 2023, le kit haltères 10kg a été retiré des rayons de 17 magasins tests.
L'objectif : évaluer si les ventes de produits alternatifs (kits 20kg, disques unitaires)
compensent la perte de CA.

Ce pipeline produit la table `mart_experiment_weekly_sales`, exposée au BI Engineer via Tableau.

---

## Architecture end-to-end

[Systèmes sources]
↓
[S3 (raw data)]                — stockage des données brutes sur AWS
↓
[Databricks]                   — ingestion et chargement dans le Delta Lake
↓
[DWH Databricks - domyos_dwh]  — tables sources (dim_model, dim_store, fact_sales, fact_stock)
↓
[dbt]                          — transformations et production du mart
↓
[mart_experiment_weekly_sales]
↓
[Tableau]                      — dashboard BI Engineer

### Extraction

Les données sources sont extraites depuis les systèmes opérationnels Decathlon et chargées dans S3 en raw. Databricks prend ensuite en charge l'ingestion vers le Delta Lake via des jobs Spark/Python planifiés. Les 4 tables sources sont disponibles dans le schéma `domyos_dwh` du DWH Databricks.

dbt se connecte directement au DWH Databricks via le connecteur `dbt-databricks` et prend en charge toutes les transformations en aval.

---

## Structure du repo

├── models/
│   ├── sources/
│   │   └── sources.yml                       # Déclaration des 4 tables sources + tests
│   ├── intermediate/
│   │   ├── int_experiment_sales_enriched.sql
│   │   └── intermediate.yml
│   └── marts/
│       ├── mart_experiment_weekly_sales.sql
│       └── marts.yml
├── setup_db.py                               # Script de création de la base DuckDB locale
├── packages.yml
└── dbt_project.yml

---

## Choix d'architecture — Pas de couche staging

Dans une architecture dbt classique, une couche staging est utilisée pour nettoyer et typer les données sources avant de les transformer.
Ce choix a été délibérément écarté ici pour les raisons suivantes :

- Les 4 tables sources (`dim_model`, `dim_store`, `fact_sales`, `fact_stock`) sont déjà disponibles dans la couche marts du DWH Databricks — elles sont donc déjà nettoyées, typées et fiables en amont
- Ajouter une couche staging aurait introduit de la complexité sans valeur ajoutée réelle sur ce périmètre
- Le modèle `int_experiment_sales_enriched` joue le rôle de couche de préparation : il filtre, enrichit et catégorise les données avant l'agrégation finale

Dans un contexte où les sources seraient brutes (fichiers S3 raw, données non nettoyées), une couche staging aurait été nécessaire pour gérer le typage, les renommages et la déduplication avant toute transformation.

## Modèles

### `int_experiment_sales_enriched`

- **Sources** : `fact_sales` × `dim_model` × `dim_store` × `fact_stock`
- **Filtre** : `item_operation_type = 'sale'` uniquement — retours et annulations exclus
- **Logique clé** : catégorise chaque produit en `kit_10kg`, `alternative` ou `other`
- **Matérialisation** : incremental sur `transaction_date` en production
- **Note** : le `inner join` sur `dim_model` exclut silencieusement les transactions dont l'`item_code` n'existe pas dans le référentiel produit

### `mart_experiment_weekly_sales`

- **Grain** : 1 ligne par `year × week × store_code × transaction_channel_type`
- **Source** : `int_experiment_sales_enriched`
- **Matérialisation** : full refresh hebdomadaire


| Colonne                    | Description                                      |
| -------------------------- | ------------------------------------------------ |
| `is_test_period`           | TRUE si S35–S41 2023                             |
| `is_tested_region`         | TRUE = groupe test (17 magasins)                 |
| `transaction_channel_type` | Canal de vente (offline / online)                |
| `gmv_kit_10kg`             | CA brut du produit retiré                        |
| `gmv_alternatives`         | CA brut des produits alternatifs                 |
| `gmv_total`                | CA brut total du magasin                         |
| `gmv_per_sqm`              | GMV normalisée par m² de surface                 |
| `availability_rate`        | Taux de disponibilité stock moyen sur la semaine |
| `net_uplift_estimated`     | GMV alternatives - GMV kit 10kg                  |


---

## Setup local (DuckDB)

```bash
# 1. Installer les dépendances
pip install dbt-core dbt-duckdb duckdb==0.9.2 pandas
# 2. Installer les packages dbt
dbt deps
# 3. Placer les CSV sources dans seeds/
# (dim_model.csv, dim_store.csv, fact_sales.csv, fact_stock.csv)
# 4. Créer la base DuckDB
python setup_db.py
# 5. Lancer le pipeline
dbt run
dbt test
```

---

## Orchestration (production)

Sur la stack Decathlon (Airflow + Databricks + AWS), le DAG tourne **chaque lundi à 06h00** :
[dbt source freshness]
↓
[dbt run --select int_experiment_sales_enriched]   — incremental
↓
[dbt run --select mart_experiment_weekly_sales]    — full refresh
↓
[dbt test --select mart_experiment_weekly_sales]
↓
[Notification Slack #data-team]

---

## Qualité des données

- Certains `store_code` présents dans `fact_sales` n'ont pas de correspondance dans `dim_store`.
Ces transactions restent conservées dans le dataset afin d'éviter toute perte de GMV,
mais les attributs magasin (`is_tested_region`, `location`, `sales_area`) seront NULL.

---

## Limites et biais à connaître

1. **Biais de stock** : `availability_rate` reflète le stock système, pas la présence physique en rayon. Un magasin peut avoir du stock en réserve sans exposer le produit.
2. **Non-comparabilité des groupes** :

Le test porte sur un nombre limité de magasins testés (~17) comparés à un groupe contrôle beaucoup plus large.
Les différences structurelles (taille, assortiment, localisation) peuvent biaiser les comparaisons directes.
Utiliser `gmv_selected_products_per_sqm` et segmenter par `family_range`
permet de comparer des magasins aux profils plus homogènes.

1. **Saisonnalité** : S35-S41 correspond à la rentrée, période naturellement forte pour le sport. Une comparaison avec les mêmes semaines en 2022 renforcerait l'analyse.
2. **Lost demand** : les clients repartis sans acheter ne sont pas visibles dans les données transactionnelles. La perte réelle peut être sous-estimée.
3. `**net_uplift_estimated`** : uniquement pertinent pour `is_tested_region = TRUE` pendant `is_test_period = TRUE`. Dans les stores contrôle il sera toujours positif puisque le kit 10kg se vend normalement.

