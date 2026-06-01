# Data Model — Spotify Platform

## ERD (simplifié)

artists (1) ──< albums (1) ──< tracks
artists (1) ──< tracks
tracks (1) ──< listening_events
tracks (1) ──< daily_streams
tracks (1) ──< realtime_top_tracks
peers (1) ──< listening_events
tracks (1) ──< recommendations

## Tables principales

| Table | Rôle |
|---|---|
| artists / albums / tracks | Catalogue musical |
| listening_events | Événements d'écoute P2P |
| daily_streams | Agrégats batch (Airflow) |
| realtime_top_tracks | Agrégats streaming (Spark) |
| dead_letter_events | Événements en erreur |
| recommendations | Recommandations utilisateur |
| federated_catalog | Catalogue inter-groupes |

## Questions d'architecture

**Pourquoi `listening_events` est indexé sur `timestamp` ET `date_trunc('hour', timestamp)` ?**
Le premier index couvre les requêtes par plage de dates (WHERE timestamp BETWEEN ...).
Le second couvre les agrégations horaires fréquentes faites par Airflow et Spark,
évitant un recalcul du tronc à chaque ligne scannée.

**Différence entre `daily_streams` (batch) et `realtime_top_tracks` (Spark) ?**
`daily_streams` est calculé une fois par jour par un DAG Airflow : données stables,
idéales pour les rapports. `realtime_top_tracks` est alimenté en continu par Spark
Structured Streaming avec des fenêtres de 5 minutes : données live pour le Top 50.

**Pourquoi `dead_letter_events.payload` est JSONB plutôt que TEXT ?**
JSONB permet d'interroger et d'indexer des champs internes au payload
(ex: payload->>'track_id'), de valider la structure à l'insertion,
et d'utiliser les opérateurs JSON natifs PostgreSQL. TEXT serait opaque et non requêtable.
