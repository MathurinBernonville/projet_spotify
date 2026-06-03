# RUNBOOK — Spotify Data Platform

## Incident 1 — Airflow DAG bloqué en running

### Symptômes
Un DAG reste en statut "running" depuis plus de 30 minutes sans progresser.

### Causes probables
- Worker Celery surchargé ou crashé
- Connexion PostgreSQL saturée

### Procédure
1. Vérifier les logs : `docker compose logs airflow-worker -f`
2. Dans l'UI Airflow : cliquer sur la tâche bloquée → Clear
3. Si le worker est down : `docker compose restart airflow-worker`
4. Vérifier les connexions PostgreSQL :
   `SELECT count(*), state FROM pg_stat_activity GROUP BY state;`
5. Terminer les connexions idle si nécessaire :
   `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle';`

---

## Incident 2 — Buffer Redis vide, listening_events non alimenté

### Symptômes
`SELECT COUNT(*) FROM listening_events` retourne 0 après un run du DAG streaming_events_pipeline.

### Causes probables
- Le simulateur P2P n'est pas lancé
- Redis a redémarré (données en mémoire perdues)
- Le simulateur écrit sur une mauvaise base Redis

### Procédure
1. Vérifier le buffer : `docker exec -it cours_hetic-redis-1 redis-cli -n 1 llen listening_events_buffer`
2. Si vide, relancer le simulateur : `python -m src.p2p_simulator.simulator --peers 10 --rate 5`
3. Laisser tourner 30 secondes puis retrigger le DAG
4. Vérifier que le catalogue est chargé : `SELECT COUNT(*) FROM tracks;`
   Si 0, relancer catalog_ingestion_pipeline d'abord.

---

## Incident 3 — Spark OutOfMemoryError (Phase 2)

### Symptômes
Les jobs Spark tombent avec OutOfMemoryError dans les logs.

### Causes probables
- SPARK_WORKER_MEMORY trop élevé pour la machine
- Trop de partitions chargées simultanément

### Procédure
1. Vérifier les logs : `docker compose -f docker-compose.yml -f docker-compose.kafka.yml logs spark-worker-1`
2. Réduire la mémoire dans docker-compose.kafka.yml :
   `SPARK_WORKER_MEMORY: 1G`
3. Redémarrer les workers :
   `docker compose -f docker-compose.yml -f docker-compose.kafka.yml restart spark-worker-1 spark-worker-2`
4. Si persistant, réduire le batch size dans les jobs Spark.
