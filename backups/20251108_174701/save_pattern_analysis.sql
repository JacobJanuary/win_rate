                                                                pg_get_functiondef                                                                 
---------------------------------------------------------------------------------------------------------------------------------------------------
 CREATE OR REPLACE FUNCTION fas_v2.save_pattern_analysis(p_period character varying, p_min_wr numeric DEFAULT 30.0, p_debug boolean DEFAULT false)+
  RETURNS TABLE(saved_count integer, execution_time_ms bigint, message text)                                                                      +
  LANGUAGE plpgsql                                                                                                                                +
 AS $function$                                                                                                                                    +
 DECLARE                                                                                                                                          +
     v_pattern RECORD;                                                                                                                            +
     v_basic_result RECORD;                                                                                                                       +
     v_detailed_result RECORD;                                                                                                                    +
     v_saved_count INTEGER := 0;                                                                                                                  +
     v_period_start DATE;                                                                                                                         +
     v_period_end DATE;                                                                                                                           +
     v_start_time TIMESTAMP;                                                                                                                      +
     v_lock_acquired BOOLEAN;                                                                                                                     +
 BEGIN                                                                                                                                            +
     v_start_time := clock_timestamp();                                                                                                           +
                                                                                                                                                  +
     -- Защита от параллельных запусков                                                                                                           +
     v_lock_acquired := pg_try_advisory_lock(hashtext('save_pattern_analysis_' || p_period));                                                     +
                                                                                                                                                  +
     IF NOT v_lock_acquired THEN                                                                                                                  +
         RETURN QUERY SELECT                                                                                                                      +
             0::INTEGER,                                                                                                                          +
             0::BIGINT,                                                                                                                           +
             'Another instance is already running for period ' || p_period;                                                                       +
         RETURN;                                                                                                                                  +
     END IF;                                                                                                                                      +
                                                                                                                                                  +
     BEGIN                                                                                                                                        +
         -- Определяем период                                                                                                                     +
         IF p_period = 'week' THEN                                                                                                                +
             v_period_start := CURRENT_DATE - INTERVAL '9 days';                                                                                  +
             v_period_end := CURRENT_DATE - INTERVAL '2 days';                                                                                    +
         ELSE                                                                                                                                     +
             v_period_start := CURRENT_DATE - INTERVAL '32 days';                                                                                 +
             v_period_end := CURRENT_DATE - INTERVAL '2 days';                                                                                    +
         END IF;                                                                                                                                  +
                                                                                                                                                  +
         IF p_debug THEN                                                                                                                          +
             RAISE NOTICE 'Processing period: % to %, min WR: %',                                                                                 +
                 v_period_start, v_period_end, p_min_wr;                                                                                          +
         END IF;                                                                                                                                  +
                                                                                                                                                  +
         -- ИЗМЕНЕНИЕ: Полная очистка таблиц (актуальны только последние данные)                                                                  +
         IF p_period = 'week' THEN                                                                                                                +
             IF p_debug THEN                                                                                                                      +
                 RAISE NOTICE 'Truncating web.pattern_analysis_weekly...';                                                                        +
             END IF;                                                                                                                              +
             TRUNCATE web.pattern_analysis_weekly;                                                                                                +
         ELSE                                                                                                                                     +
             IF p_debug THEN                                                                                                                      +
                 RAISE NOTICE 'Truncating web.pattern_analysis_monthly...';                                                                       +
             END IF;                                                                                                                              +
             TRUNCATE web.pattern_analysis_monthly;                                                                                               +
         END IF;                                                                                                                                  +
                                                                                                                                                  +
         -- Получаем все доступные паттерны за период                                                                                             +
         FOR v_pattern IN                                                                                                                         +
             SELECT DISTINCT sp.pattern_type::text as pattern_name                                                                                +
             FROM fas_v2.signal_patterns sp                                                                                                       +
             INNER JOIN fas_v2.sh_patterns shp ON sp.id = shp.signal_patterns_id                                                                  +
             INNER JOIN fas_v2.scoring_history sh ON shp.scoring_history_id = sh.id                                                               +
             WHERE sh.timestamp BETWEEN v_period_start AND v_period_end                                                                           +
         LOOP                                                                                                                                     +
             IF p_debug THEN                                                                                                                      +
                 RAISE NOTICE 'Processing pattern: %', v_pattern.pattern_name;                                                                    +
             END IF;                                                                                                                              +
                                                                                                                                                  +
             -- Базовый анализ для каждого паттерна                                                                                               +
             IF p_period = 'week' THEN                                                                                                            +
                 FOR v_basic_result IN                                                                                                            +
                     SELECT dir, mreg, tf, tot_sig, win_sig, wr                                                                                   +
                     FROM fas_v2.analyze_pattern_weekly(v_pattern.pattern_name) t(dir, mreg, tf, tot_sig, win_sig, wr)                            +
                     WHERE t.wr > p_min_wr                                                                                                        +
                 LOOP                                                                                                                             +
                     -- Детальный анализ                                                                                                          +
                     FOR v_detailed_result IN                                                                                                     +
                         SELECT pname, tframe, dir, mreg, addpat, totsig, winsig, wr                                                              +
                         FROM fas_v2.analyze_pattern_detailed_weekly(                                                                             +
                             v_pattern.pattern_name,                                                                                              +
                             v_basic_result.dir,                                                                                                  +
                             v_basic_result.mreg,                                                                                                 +
                             v_basic_result.tf                                                                                                    +
                         ) t(pname, tframe, dir, mreg, addpat, totsig, winsig, wr)                                                                +
                     LOOP                                                                                                                         +
                         INSERT INTO web.pattern_analysis_weekly (                                                                                +
                             pattern_name, direction, market_regime, timeframe,                                                                   +
                             combination_type, pattern_list, total_signals,                                                                       +
                             winning_signals, win_rate, period_start, period_end                                                                  +
                         ) VALUES (                                                                                                               +
                             v_detailed_result.pname,                                                                                             +
                             v_detailed_result.dir,                                                                                               +
                             v_detailed_result.mreg,                                                                                              +
                             v_detailed_result.tframe,                                                                                            +
                             v_detailed_result.addpat,                                                                                            +
                             CASE                                                                                                                 +
                                 WHEN v_detailed_result.addpat = 'None' THEN                                                                      +
                                     ARRAY[v_detailed_result.pname]                                                                               +
                                 WHEN v_detailed_result.addpat LIKE 'Higher_TF%' THEN                                                             +
                                     ARRAY[v_detailed_result.pname]                                                                               +
                                 ELSE                                                                                                             +
                                     string_to_array(                                                                                             +
                                         regexp_replace(v_detailed_result.addpat, '\s*\+\s*', ' + ', 'g'),                                        +
                                         ' + '                                                                                                    +
                                     )                                                                                                            +
                             END,                                                                                                                 +
                             v_detailed_result.totsig,                                                                                            +
                             v_detailed_result.winsig,                                                                                            +
                             v_detailed_result.wr,                                                                                                +
                             v_period_start,                                                                                                      +
                             v_period_end                                                                                                         +
                         ) ON CONFLICT DO NOTHING;                                                                                                +
                         v_saved_count := v_saved_count + 1;                                                                                      +
                     END LOOP;                                                                                                                    +
                 END LOOP;                                                                                                                        +
             ELSE  -- month                                                                                                                       +
                 FOR v_basic_result IN                                                                                                            +
                     SELECT dir, mreg, tf, tot_sig, win_sig, wr                                                                                   +
                     FROM fas_v2.analyze_pattern_monthly(v_pattern.pattern_name) t(dir, mreg, tf, tot_sig, win_sig, wr)                           +
                     WHERE t.wr > p_min_wr                                                                                                        +
                 LOOP                                                                                                                             +
                     FOR v_detailed_result IN                                                                                                     +
                         SELECT pname, tframe, dir, mreg, addpat, totsig, winsig, wr                                                              +
                         FROM fas_v2.analyze_pattern_detailed_monthly(                                                                            +
                             v_pattern.pattern_name,                                                                                              +
                             v_basic_result.dir,                                                                                                  +
                             v_basic_result.mreg,                                                                                                 +
                             v_basic_result.tf                                                                                                    +
                         ) t(pname, tframe, dir, mreg, addpat, totsig, winsig, wr)                                                                +
                     LOOP                                                                                                                         +
                         INSERT INTO web.pattern_analysis_monthly (                                                                               +
                             pattern_name, direction, market_regime, timeframe,                                                                   +
                             combination_type, pattern_list, total_signals,                                                                       +
                             winning_signals, win_rate, period_start, period_end                                                                  +
                         ) VALUES (                                                                                                               +
                             v_detailed_result.pname,                                                                                             +
                             v_detailed_result.dir,                                                                                               +
                             v_detailed_result.mreg,                                                                                              +
                             v_detailed_result.tframe,                                                                                            +
                             v_detailed_result.addpat,                                                                                            +
                             CASE                                                                                                                 +
                                 WHEN v_detailed_result.addpat = 'None' THEN                                                                      +
                                     ARRAY[v_detailed_result.pname]                                                                               +
                                 WHEN v_detailed_result.addpat LIKE 'Higher_TF%' THEN                                                             +
                                     ARRAY[v_detailed_result.pname]                                                                               +
                                 ELSE                                                                                                             +
                                     string_to_array(                                                                                             +
                                         regexp_replace(v_detailed_result.addpat, '\s*\+\s*', ' + ', 'g'),                                        +
                                         ' + '                                                                                                    +
                                     )                                                                                                            +
                             END,                                                                                                                 +
                             v_detailed_result.totsig,                                                                                            +
                             v_detailed_result.winsig,                                                                                            +
                             v_detailed_result.wr,                                                                                                +
                             v_period_start,                                                                                                      +
                             v_period_end                                                                                                         +
                         ) ON CONFLICT DO NOTHING;                                                                                                +
                         v_saved_count := v_saved_count + 1;                                                                                      +
                     END LOOP;                                                                                                                    +
                 END LOOP;                                                                                                                        +
             END IF;                                                                                                                              +
         END LOOP;                                                                                                                                +
                                                                                                                                                  +
         -- Снимаем блокировку                                                                                                                    +
         PERFORM pg_advisory_unlock(hashtext('save_pattern_analysis_' || p_period));                                                              +
                                                                                                                                                  +
         RETURN QUERY SELECT                                                                                                                      +
             v_saved_count,                                                                                                                       +
             EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time)::BIGINT,                                                                 +
             format('Successfully saved %s records for %s period', v_saved_count, p_period);                                                      +
                                                                                                                                                  +
     EXCEPTION WHEN OTHERS THEN                                                                                                                   +
         -- Снимаем блокировку в случае ошибки                                                                                                    +
         PERFORM pg_advisory_unlock(hashtext('save_pattern_analysis_' || p_period));                                                              +
         RAISE NOTICE 'Error in save_pattern_analysis: %', SQLERRM;                                                                               +
         RETURN QUERY SELECT                                                                                                                      +
             0::INTEGER,                                                                                                                          +
             0::BIGINT,                                                                                                                           +
             'Error: ' || SQLERRM;                                                                                                                +
     END;                                                                                                                                         +
 END;                                                                                                                                             +
 $function$                                                                                                                                       +
 
(1 row)

