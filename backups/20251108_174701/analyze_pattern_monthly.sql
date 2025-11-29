                                                                            pg_get_functiondef                                                                             
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 CREATE OR REPLACE FUNCTION fas_v2.analyze_pattern_monthly(p_pattern_name character varying)                                                                              +
  RETURNS TABLE(direction character varying, market_regime character varying, timeframe character varying, total_signals bigint, winning_signals bigint, win_rate numeric)+
  LANGUAGE plpgsql                                                                                                                                                        +
 AS $function$                                                                                                                                                            +
 BEGIN                                                                                                                                                                    +
     -- LONG анализ                                                                                                                                                       +
     RETURN QUERY                                                                                                                                                         +
     SELECT                                                                                                                                                               +
         'LONG'::VARCHAR as direction,                                                                                                                                    +
         COALESCE(mr.regime, 'UNKNOWN')::VARCHAR as market_regime,                                                                                                        +
         sp.timeframe::VARCHAR as timeframe,                                                                                                                              +
         COUNT(CASE WHEN res.is_win IS NOT NULL THEN 1 END) as total_signals,                                                                                             +
         COUNT(CASE WHEN res.is_win = true THEN 1 END) as winning_signals,                                                                                                +
         ROUND(COUNT(CASE WHEN res.is_win = true THEN 1 END)::numeric /                                                                                                   +
               NULLIF(COUNT(CASE WHEN res.is_win IS NOT NULL THEN 1 END), 0) * 100, 1) as win_rate                                                                        +
     FROM fas_v2.scoring_history sh                                                                                                                                       +
     INNER JOIN fas_v2.sh_patterns shp ON sh.id = shp.scoring_history_id                                                                                                  +
     INNER JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id                                                                                               +
     LEFT JOIN fas_v2.sh_regime shr ON sh.id = shr.scoring_history_id                                                                                                     +
     LEFT JOIN fas_v2.market_regime mr ON shr.signal_regime_id = mr.id                                                                                                    +
     LEFT JOIN web.scoring_history_results_v2 res                                                                                                                         +
         ON sh.id = res.scoring_history_id                                                                                                                                +
         AND res.signal_type = 'LONG'                                                                                                                                     +
     WHERE sp.pattern_type = p_pattern_name::fas_v2.signal_pattern_type                                                                                                   +
         AND sp.score_impact > 0                                                                                                                                          +
         AND sh.timestamp <= CURRENT_DATE - INTERVAL '2 days'                                                                                                             +
         AND sh.timestamp >= CURRENT_DATE - INTERVAL '32 days'                                                                                                            +
     GROUP BY mr.regime, sp.timeframe                                                                                                                                     +
     HAVING COUNT(CASE WHEN res.is_win IS NOT NULL THEN 1 END) > 0                                                                                                        +
                                                                                                                                                                          +
     UNION ALL                                                                                                                                                            +
                                                                                                                                                                          +
     -- SHORT анализ                                                                                                                                                      +
     SELECT                                                                                                                                                               +
         'SHORT'::VARCHAR as direction,                                                                                                                                   +
         COALESCE(mr.regime, 'UNKNOWN')::VARCHAR as market_regime,                                                                                                        +
         sp.timeframe::VARCHAR as timeframe,                                                                                                                              +
         COUNT(CASE WHEN res.is_win IS NOT NULL THEN 1 END) as total_signals,                                                                                             +
         COUNT(CASE WHEN res.is_win = true THEN 1 END) as winning_signals,                                                                                                +
         ROUND(COUNT(CASE WHEN res.is_win = true THEN 1 END)::numeric /                                                                                                   +
               NULLIF(COUNT(CASE WHEN res.is_win IS NOT NULL THEN 1 END), 0) * 100, 1) as win_rate                                                                        +
     FROM fas_v2.scoring_history sh                                                                                                                                       +
     INNER JOIN fas_v2.sh_patterns shp ON sh.id = shp.scoring_history_id                                                                                                  +
     INNER JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id                                                                                               +
     LEFT JOIN fas_v2.sh_regime shr ON sh.id = shr.scoring_history_id                                                                                                     +
     LEFT JOIN fas_v2.market_regime mr ON shr.signal_regime_id = mr.id                                                                                                    +
     LEFT JOIN web.scoring_history_results_v2 res                                                                                                                         +
         ON sh.id = res.scoring_history_id                                                                                                                                +
         AND res.signal_type = 'SHORT'                                                                                                                                    +
     WHERE sp.pattern_type = p_pattern_name::fas_v2.signal_pattern_type                                                                                                   +
         AND sp.score_impact < 0                                                                                                                                          +
         AND sh.timestamp <= CURRENT_DATE - INTERVAL '2 days'                                                                                                             +
         AND sh.timestamp >= CURRENT_DATE - INTERVAL '32 days'                                                                                                            +
     GROUP BY mr.regime, sp.timeframe                                                                                                                                     +
     HAVING COUNT(CASE WHEN res.is_win IS NOT NULL THEN 1 END) > 0                                                                                                        +
                                                                                                                                                                          +
     ORDER BY direction, market_regime, timeframe;                                                                                                                        +
 END;                                                                                                                                                                     +
 $function$                                                                                                                                                               +
 
(1 row)

