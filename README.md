# Maintenance Prédictive des Moteurs Aéronautiques – Big Data

## 1. Présentation du projet

Ce projet consiste à concevoir une plateforme Big Data dédiée à la maintenance prédictive des moteurs aéronautiques.

L'objectif est d'exploiter les données de capteurs afin de suivre l'évolution de l'état des moteurs, analyser leur dégradation et estimer leur durée de vie utile restante (RUL – Remaining Useful Life).

Le projet utilise le dataset **NASA C-MAPSS FD001**, un jeu de données de simulation de dégradation des moteurs turbofan.

## 2. Architecture technique

Les technologies utilisées dans le projet sont :

| Technologie | Rôle |
|---|---|
| Python | Ingestion et préparation des données |
| Apache Kafka | Streaming des données |
| MinIO | Stockage objet compatible S3 |
| Apache Hudi | Gestion des données dans le Lakehouse |
| Apache Spark | Traitement distribué des données |
| Spark MLlib | Machine Learning et prédiction |
| Apache Airflow | Orchestration des traitements |
| Grafana | Visualisation et monitoring |
| Docker Compose | Gestion des services |

## 3. Organisation du projet

```text
BIGDATA-ISSE02/
│
├── docker-compose.yml
│
├── infrastructure/
│   ├── kafka/
│   ├── minio/
│   ├── spark/
│   ├── airflow/
│   └── grafana/
│
├── ingestion/
│   ├── producer/
│   └── bronze-consumer/
│
├── data/
│   ├── raw/
│   │   └── nasa_cmapps/
│   └── processed/
│
├── lakehouse/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── spark/
│   └── jobs/
│
├── airflow/
│   └── dags/
│
├── dashboard/
│
├── src/
│   └── analysis/
│
├── tests/
│
└── docs/
    └── report/
```

## 4. Répartition du travail

### Étudiant 1 – Ingestion des données

Responsabilités :
- Développement du Producer Python.
- Lecture du dataset NASA C-MAPSS.
- Envoi des données vers Apache Kafka.
- Préparation de l'ingestion des données.

Dossiers concernés :
- `ingestion/producer/`
- `src/analysis/`
- `infrastructure/kafka/`

### Étudiant 2 – Stockage et Lakehouse

Responsabilités :
- Configuration de MinIO.
- Mise en place d'Apache Hudi.
- Organisation des couches Bronze, Silver et Gold.
- Stockage et gestion des données.

Dossiers concernés :
- `infrastructure/minio/`
- `lakehouse/bronze/`
- `lakehouse/silver/`
- `lakehouse/gold/`

### Étudiant 3 – Traitement et Machine Learning

Responsabilités :
- Développement des traitements avec Apache Spark.
- Nettoyage et transformation des données.
- Feature Engineering.
- Développement du modèle de prédiction RUL avec Spark MLlib.

Dossiers concernés :
- `spark/jobs/`
- `infrastructure/spark/`

### Étudiant 4 – Orchestration et Visualisation

Responsabilités :
- Orchestration des traitements avec Apache Airflow.
- Création des DAGs.
- Mise en place des dashboards Grafana.
- Monitoring du pipeline Big Data.

Dossiers concernés :
- `airflow/dags/`
- `dashboard/`
- `infrastructure/airflow/`
- `infrastructure/grafana/`

## 5. Exécution du projet

### Prérequis

- Docker Desktop
- Docker Compose
- Python
- Git

### Démarrage de Kafka

Depuis la racine du projet :

```bash
docker compose up -d
```

Vérifier que le conteneur Kafka fonctionne :

```bash
docker ps
```

Arrêter les services :

```bash
docker compose down
```

Le fichier `docker-compose.yml` à la racine contient actuellement la configuration de Kafka. Les autres services seront intégrés progressivement.

## 6. Dataset

Le projet utilise le dataset NASA C-MAPSS FD001.

Les fichiers de données sont placés dans :

`data/raw/nasa_cmapps/`

Ce dataset contient des trajectoires de moteurs turbofan simulées et des mesures de capteurs utilisées pour étudier la dégradation des moteurs.

## 7. Collaboration Git

Le travail est organisé à travers des branches Git.

- `main` : branche principale du projet.
- `feature/kafka-ingestion` : branche de développement de l'ingestion Kafka.

Bonnes pratiques :
1. Récupérer les dernières modifications avant de commencer.
2. Travailler sur sa propre branche.
3. Utiliser des commits avec des messages explicites.
4. Ne pas modifier directement la branche `main` sans accord du groupe.
5. Tester les modifications avant de les pousser sur GitHub.

## 8. État d'avancement

Le projet est en cours de développement.

Les premières étapes réalisées comprennent :
- Préparation et inspection du dataset NASA C-MAPSS FD001.
- Mise en place de Kafka avec Docker Compose.
- Développement et test du Producer Python.
- Organisation initiale de l'arborescence du projet.
- Mise en place du dépôt GitHub et de la branche de développement.

Les autres composants seront intégrés progressivement par les membres du groupe.

---

**Projet Big Data – Master IISE**  
**Maintenance Prédictive des Moteurs Aéronautiques**
