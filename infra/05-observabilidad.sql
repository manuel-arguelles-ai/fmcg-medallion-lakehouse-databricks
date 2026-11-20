-- Observabilidad de plataforma con system tables (solo lectura; requiere acceso a system.* en el metastore).
-- Uso: python3 infra/run_sql.py infra/05-observabilidad.sql --warehouse <id>  (o pegar cada consulta en el editor SQL)

-- 1) Corridas de jobs del proyecto: duracion y resultado
SELECT j.name AS job, t.period_start_time, t.period_end_time, t.result_state,
       timestampdiff(MINUTE, t.period_start_time, t.period_end_time) AS minutos
FROM system.lakeflow.job_run_timeline t
JOIN system.lakeflow.jobs j ON j.workspace_id = t.workspace_id AND j.job_id = t.job_id
WHERE j.name LIKE '%fmcg%'
ORDER BY t.period_start_time DESC
LIMIT 50;

-- 2) Costo en DBU por job del proyecto (ultimos 30 dias)
SELECT u.usage_date, u.sku_name, u.usage_metadata.job_id AS job_id, SUM(u.usage_quantity) AS dbus
FROM system.billing.usage u
WHERE u.usage_date >= date_sub(current_date(), 30) AND u.usage_metadata.job_id IS NOT NULL
GROUP BY ALL
ORDER BY u.usage_date DESC;

-- 3) Estado de reconciliacion entre capas (propio del proyecto)
SELECT * FROM fmcg_lakehouse.ops.pipeline_runs ORDER BY 1 DESC LIMIT 50;
