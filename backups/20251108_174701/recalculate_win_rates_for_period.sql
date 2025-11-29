                                           pg_get_functiondef                                           
--------------------------------------------------------------------------------------------------------
 CREATE OR REPLACE FUNCTION fas_v2.recalculate_win_rates_for_period(p_days_ago integer DEFAULT 30)     +
  RETURNS TABLE(signals_processed bigint, duration_seconds numeric)                                    +
  LANGUAGE plpgsql                                                                                     +
 AS $function$                                                                                         +
 DECLARE                                                                                               +
     v_start_time TIMESTAMPTZ := clock_timestamp();                                                    +
     v_start_date TIMESTAMPTZ;                                                                         +
     v_signal_record RECORD;                                                                           +
     v_processed_count BIGINT := 0;                                                                    +
 BEGIN                                                                                                 +
     v_start_date := NOW() - (p_days_ago || ' days')::INTERVAL;                                        +
                                                                                                       +
     RAISE NOTICE 'Starting recalculation of weekly/monthly scores for the last % days...', p_days_ago;+
     RAISE NOTICE 'Processing signals from % onwards.', v_start_date;                                  +
                                                                                                       +
     -- Начинаем цикл по всем записям в scoring_history за указанный период                            +
     FOR v_signal_record IN                                                                            +
         SELECT id                                                                                     +
         FROM fas_v2.scoring_history                                                                   +
         WHERE "timestamp" >= v_start_date                                                             +
         ORDER BY "timestamp" ASC -- Обрабатываем от старых к новым                                    +
     LOOP                                                                                              +
         -- Для каждой записи вызываем функцию обновления WR из fas_v2                                 +
         -- PERFORM используется, так как нам не нужно обрабатывать результат,                         +
         -- который возвращает функция, нам важен только UPDATE внутри неё.                            +
         PERFORM fas_v2.update_scoring_history_wr(v_signal_record.id);                                 +
                                                                                                       +
         v_processed_count := v_processed_count + 1;                                                   +
                                                                                                       +
         -- Выводим прогресс каждые 500 обработанных сигналов                                          +
         IF v_processed_count % 500 = 0 THEN                                                           +
             RAISE NOTICE 'Processed % signals...', v_processed_count;                                 +
         END IF;                                                                                       +
     END LOOP;                                                                                         +
                                                                                                       +
     RAISE NOTICE 'Recalculation complete. Total signals processed: %', v_processed_count;             +
                                                                                                       +
     -- Возвращаем итоговую статистику                                                                 +
     RETURN QUERY                                                                                      +
     SELECT                                                                                            +
         v_processed_count,                                                                            +
         ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::NUMERIC, 2);                    +
 END;                                                                                                  +
 $function$                                                                                            +
 
(1 row)

