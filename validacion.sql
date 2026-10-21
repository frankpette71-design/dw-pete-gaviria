-- ============================================================
-- Consultas de validación (sección 4.5 del informe)
-- Ya vienen con el código de equipo dw_pete_gaviria: se pueden
-- ejecutar tal cual en la consola de BigQuery.
-- ============================================================

-- 1) Conteos sobre hechos y dimensiones (R6)
SELECT 'fact_listings'        AS tabla, COUNT(*) AS filas FROM `dw_pete_gaviria_dwh.fact_listings`
UNION ALL
SELECT 'dim_barrio',           COUNT(*) FROM `dw_pete_gaviria_dwh.dim_barrio`
UNION ALL
SELECT 'dim_anfitrion',        COUNT(*) FROM `dw_pete_gaviria_dwh.dim_anfitrion`
UNION ALL
SELECT 'dim_tipo_alojamiento', COUNT(*) FROM `dw_pete_gaviria_dwh.dim_tipo_alojamiento`
ORDER BY tabla;

-- 2) Pregunta de negocio: ¿en qué barrios conviene enfocar una campaña
--    para anfitriones? (precio promedio, demanda reciente e ingreso potencial)
SELECT
  b.barrio,
  COUNT(*)                               AS anuncios,
  ROUND(AVG(f.precio), 0)                AS precio_promedio,
  SUM(f.resenas_12m)                     AS resenas_ultimo_anio,
  ROUND(AVG(f.ingreso_potencial_anual), 0) AS ingreso_potencial_promedio
FROM `dw_pete_gaviria_dwh.fact_listings` f
JOIN `dw_pete_gaviria_dwh.dim_barrio` b USING (barrio_key)
GROUP BY b.barrio
HAVING anuncios >= 30
ORDER BY resenas_ultimo_anio DESC
LIMIT 10;

-- 3) Pregunta de negocio: ¿los superanfitriones cobran más y tienen mejor calificación?
SELECT
  a.es_superhost,
  t.tipo_alojamiento,
  COUNT(*)                    AS anuncios,
  ROUND(AVG(f.precio), 0)     AS precio_promedio,
  ROUND(AVG(f.calificacion), 2) AS calificacion_promedio
FROM `dw_pete_gaviria_dwh.fact_listings` f
JOIN `dw_pete_gaviria_dwh.dim_anfitrion` a        USING (anfitrion_key)
JOIN `dw_pete_gaviria_dwh.dim_tipo_alojamiento` t USING (tipo_key)
GROUP BY a.es_superhost, t.tipo_alojamiento
ORDER BY t.tipo_alojamiento, a.es_superhost;
