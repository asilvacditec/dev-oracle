create or replace package dba_utl as


  procedure gera_linha;

  procedure periodo_analise;

  procedure alpakill;
  
  procedure count_tables;
  
  procedure time_query;


end dba_utl;

create or replace package body dba_utl as
  

  procedure gera_linha is
  linha varchar2(1000);
  begin
    for x in 1..60 loop
      linha := linha || '*=';
    end loop;
    dbms_output.put_line('');
    dbms_output.put_line(linha);
  end gera_linha;

  procedure periodo_analise is
    cursor sql3 is
    select max(to_char(sample_time,'dd/mm/rr hh24:mi:ss')) max,
       min(to_char(sample_time,'dd/mm/rr hh24:mi:ss')) min 
    from v$active_session_history;
  begin
    for periodo in sql3 loop
      dbms_output.put_line('Período de análise de ' || periodo.min|| ' à ' || periodo.max);
    end loop;
  end periodo_analise;

  procedure alpakill is
    cursor c_sessoes is
      SELECT sid, serial# FROM v$session WHERE username = 'ALPAMSAF';
    sqltext varchar(100);
  begin
    for r in c_sessoes loop
      sqltext := 'ALTER SYSTEM KILL SESSION '''|| r.sid || ',' || r.serial# || ''' IMMEDIATE';
      execute immediate sqltext;
    end loop;
  end alpakill;
  
  procedure count_tables is
    type cur_typ is ref cursor;
    c2 cur_typ;
    sql2 varchar2(200);
    varqtd varchar2(30);
    v_time pls_integer;

    cursor sql1 is 
      select * from dba_tab_statistics 
      where owner = 'ALPAMSAF' and stale_stats = 'YES';

    begin
      v_time := dbms_utility.get_time;
      dbms_output.put_line('Inicio: ' || to_char(sysdate));
      periodo_analise;
      dba_utl.gera_linha;
      for c1 in sql1 loop
        sql2 := 'select count(*) qtd from ' || c1.owner || '.' || c1.table_name || ' order by qtd' ;
        open c2 for sql2; loop
          fetch c2 into varqtd;
          exit when c2%NOTFOUND;
          -- Processa somente os registros que possuem quantidade diferente desde a última coleta de estaticas
          if to_number(c1.num_rows) <> to_number(varqtd) then
            dbms_output.put_line(
              'Table name: ' || rpad(c1.owner || '.' || c1.table_name,35) || 
              ', NumRows: ' || lpad(c1.num_rows,10) || 
              ', Count: ' || lpad(varqtd,10) || 
              ', Diff: ' || lpad(to_char(to_number(varqtd) - to_number(c1.num_rows)),10)  ||
              ', Last Analyzed: ' || to_char(c1.last_analyzed)
            );
            /*
            DBMS_STATS.GATHER_TABLE_STATS(
                OWNNAME=> c1.owner, 
                TABNAME=> c1.table_name,
                ESTIMATE_PERCENT =>30,
                METHOD_OPT => 'FOR ALL INDEXED COLUMNS SIZE SKEWONLY',
                DEGREE => 8 ,
                GRANULARITY => 'ALL',
                CASCADE => TRUE);
            */
          end if;
        end loop;
        close c2;
      end loop;
      dba_utl.gera_linha;
      dbms_output.put_line('Fim: ' || to_char(sysdate));
      
      if (dbms_utility.get_time - v_time)/100 > 60 then
        dbms_output.put_line( round( ((dbms_utility.get_time - v_time)/100/60),2) || ' minutos');
      else
        dbms_output.put_line( round( ((dbms_utility.get_time - v_time)/100),2) || ' seconds');
      end if;

    end count_tables;
    
    
    procedure time_query is

        v_sqlid gv$sql_plan.sql_id%type;
        v_min_avgsecs gv$sql.elapsed_time%type;
        v_max_avgsecs gv$sql.elapsed_time%type;
        v_module gv$active_session_history.module%type;
        v_qtd_sqlid integer;
        v_diff_avgsecs gv$sql.elapsed_time%type;
        v_div_avgsecs gv$sql.elapsed_time%type;
        v_asterisco char(1);
        v_time pls_integer;
    
        cursor cplans is
        WITH
        p AS (
        SELECT plan_hash_value,sql_id
        FROM gv$sql_plan
        WHERE sql_id = TRIM(v_sqlid)
        AND other_xml IS NOT NULL
        UNION
        SELECT plan_hash_value,sql_id
        FROM dba_hist_sql_plan
        WHERE sql_id = TRIM(v_sqlid)
        AND other_xml IS NOT NULL ),
        m AS (
        SELECT plan_hash_value,sql_id,
            SUM(elapsed_time)/SUM(executions) avg_et_secs
        FROM gv$sql
        WHERE sql_id = TRIM(v_sqlid)
        AND executions > 0
        GROUP BY
            plan_hash_value,sql_id ),
        a AS (
        SELECT plan_hash_value,sql_id,
            SUM(elapsed_time_total)/SUM(executions_total) avg_et_secs
        FROM dba_hist_sqlstat
        WHERE sql_id = TRIM(v_sqlid)
        AND executions_total > 0
        GROUP BY
            plan_hash_value,sql_id )
            
        SELECT p.sql_id,
            min(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) min_avg_et_secs, 
            max(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) max_avg_et_secs,
            (max(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) - 
            min(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3))) diff_avg_et_secs, 
            ROUND(max(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) / 
            NULLIF(min(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)),0),3) div_avg_et_secs, 
            count(p.sql_id) qtd_sqlid
        FROM p, m, a
        WHERE p.plan_hash_value = m.plan_hash_value(+)
        AND p.plan_hash_value = a.plan_hash_value(+)
        GROUP BY p.sql_id
        HAVING (min(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) > 0 
            AND max(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) > 0) 
            AND ((ROUND(max(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) / NULLIF(min(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)),0),3)) >= 1
            AND max(ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3)) > 1 )
            --AND count(p.sql_id) > 1
        --ORDER BY 5 desc
        ;

    begin
      v_time := dbms_utility.get_time;
      dbms_output.put_line('Inicio: ' || to_char(sysdate) || ' (from memory)');
      dba_recourse.dba_utl.periodo_analise;
      dba_recourse.dba_utl.gera_linha;
      
      -- Cabecalho
      dbms_output.put_line(
        '| ' || rpad('MODULE',22) || ' | ' ||
        rpad('SQL_ID',13) ||  ' | ' ||
        lpad('MIN_ET_SECS',13) || ' | ' ||
        lpad('MAX_ET_SECS',13) || ' | ' ||
        lpad('DIFF_ET_SECS',13) || ' | ' ||
        lpad('DIV_ET_SECS',13) || ' | ' ||
        lpad('QTD_PLANOS',11) || ' | '
      );
      
      dba_recourse.dba_utl.gera_linha;
    
      for cmodules in (select distinct module from V$ACTIVE_SESSION_HISTORY where upper(module) not like 'SQL%' and upper(module) not like 'RMAN%' )
      loop
        v_module := cmodules.module;
    
        for csessoes in (
          select sql_id,count(*) qtd
          from V$ACTIVE_SESSION_HISTORY
          where module = v_module
            and sql_plan_hash_value > 0
          group by sql_id
          having count(*) > 1
        )
    
        loop
          v_sqlid := csessoes.sql_id;
          open cplans;
          
          loop
            fetch cplans into v_sqlid,v_min_avgsecs,v_max_avgsecs,v_diff_avgsecs,v_div_avgsecs,v_qtd_sqlid;
            exit when cplans%NOTFOUND;
            
            if (v_div_avgsecs is null) then
              v_div_avgsecs := 0;
            end if;
            
            v_asterisco := '';
            if v_qtd_sqlid > 1 then
              v_asterisco := '*';
            end if;
    
              dbms_output.put_line(
                '| ' || rpad(upper(v_module),22) || ' | ' ||
                rpad(v_sqlid,13) ||  ' | ' ||
                lpad(v_min_avgsecs,13) || ' | ' ||
                lpad(v_max_avgsecs,13) || ' | ' ||
                lpad(v_diff_avgsecs,13) || ' | ' ||
                lpad(v_div_avgsecs,13) || ' | ' ||
                lpad(v_qtd_sqlid,11) || ' | ' || v_asterisco 
              );
            
          end loop;
          close cplans;
        end loop;
      end loop;
      dba_recourse.dba_utl.gera_linha;
      dbms_output.put_line('Termino: ' || to_char(sysdate));
    
      if (dbms_utility.get_time - v_time)/100 > 60 then
        dbms_output.put_line( round( ((dbms_utility.get_time - v_time)/100/60),2) || ' minutos');
      else
        dbms_output.put_line( round( ((dbms_utility.get_time - v_time)/100),2) || ' seconds');
      end if;
    
    end time_query;

end dba_utl;