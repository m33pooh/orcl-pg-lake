-- PL/SQL High-Speed Bulk Inserter for Audit Logs
-- Usage: sqlplus sys/<pwd>@//localhost:1521/FREE as sysdba @generate_audit_data.sql <COUNT> <BATCH_SIZE>
SET SERVEROUTPUT ON
SET TIMING ON
SET FEEDBACK OFF

ALTER SESSION SET CONTAINER = FREEPDB1;

DECLARE
  v_target_count NUMBER := NVL(TO_NUMBER('&1'), 1000000);
  v_batch_size   NUMBER := NVL(TO_NUMBER('&2'), 10000);
  v_inserted     NUMBER := 0;
  v_start_id     NUMBER;
  v_curr_batch   NUMBER;

  TYPE t_log_id IS TABLE OF NUMBER;
  TYPE t_subsys IS TABLE OF VARCHAR2(50);
  TYPE t_action IS TABLE OF VARCHAR2(100);
  TYPE t_user IS TABLE OF VARCHAR2(50);
  TYPE t_sev IS TABLE OF VARCHAR2(20);
  TYPE t_date IS TABLE OF TIMESTAMP;

  v_ids      t_log_id;
  v_subsys   t_subsys;
  v_actions  t_action;
  v_users    t_user;
  v_sevs     t_sev;
  v_dates    t_date;

  TYPE t_subsys_list IS VARRAY(6) OF VARCHAR2(50);
  v_subsys_names t_subsys_list := t_subsys_list('AUTH_SVC', 'PAYMENT_GW', 'ORDER_SVC', 'INVENTORY_SVC', 'USER_API', 'NOTIFICATION_SVC');
  TYPE t_act_list IS VARRAY(6) OF VARCHAR2(100);
  v_act_names t_act_list := t_act_list('TOKEN_ISSUED', 'CHECKOUT_COMPLETED', 'INVOICE_GENERATED', 'STOCK_RESERVED', 'PASSWORD_RESET', 'WEBHOOK_DISPATCHED');
  TYPE t_user_list IS VARRAY(5) OF VARCHAR2(50);
  v_user_names t_user_list := t_user_list('admin_sys', 'user_1001', 'user_1002', 'cron_worker', 'gateway_agent');
  TYPE t_sev_list IS VARRAY(5) OF VARCHAR2(20);
  v_sev_names t_sev_list := t_sev_list('INFO', 'INFO', 'INFO', 'WARN', 'ERROR');
BEGIN
  SELECT NVL(MAX(LOG_ID), 0) + 1 INTO v_start_id FROM AUDIT_APP.SYSTEM_LOGS;
  DBMS_OUTPUT.PUT_LINE('Starting bulk generation of ' || TO_CHAR(v_target_count, '999,999,999') || ' audit logs from LOG_ID=' || v_start_id);

  WHILE v_inserted < v_target_count LOOP
    v_curr_batch := LEAST(v_batch_size, v_target_count - v_inserted);

    v_ids     := t_log_id();
    v_subsys  := t_subsys();
    v_actions := t_action();
    v_users   := t_user();
    v_sevs    := t_sev();
    v_dates   := t_date();

    v_ids.EXTEND(v_curr_batch);
    v_subsys.EXTEND(v_curr_batch);
    v_actions.EXTEND(v_curr_batch);
    v_users.EXTEND(v_curr_batch);
    v_sevs.EXTEND(v_curr_batch);
    v_dates.EXTEND(v_curr_batch);

    FOR i IN 1..v_curr_batch LOOP
      v_ids(i)     := v_start_id + v_inserted + (i - 1);
      v_subsys(i)  := v_subsys_names(TRUNC(DBMS_RANDOM.VALUE(1, 7)));
      v_actions(i) := v_act_names(TRUNC(DBMS_RANDOM.VALUE(1, 7)));
      v_users(i)   := v_user_names(TRUNC(DBMS_RANDOM.VALUE(1, 6)));
      v_sevs(i)    := v_sev_names(TRUNC(DBMS_RANDOM.VALUE(1, 6)));
      v_dates(i)   := SYSTIMESTAMP;
    END LOOP;

    FORALL i IN 1..v_curr_batch
      INSERT INTO AUDIT_APP.SYSTEM_LOGS (LOG_ID, SUBSYSTEM, ACTION, PERFORMED_BY, SEVERITY, LOGGED_AT)
      VALUES (v_ids(i), v_subsys(i), v_actions(i), v_users(i), v_sevs(i), v_dates(i));

    COMMIT;
    v_inserted := v_inserted + v_curr_batch;

    IF MOD(v_inserted, 50000) = 0 OR v_inserted = v_target_count THEN
      DBMS_OUTPUT.PUT_LINE('Inserted: ' || TO_CHAR(v_inserted, '999,999,999') || ' / ' || TO_CHAR(v_target_count, '999,999,999') || ' (' || TO_CHAR(ROUND((v_inserted / v_target_count) * 100, 1)) || '%)');
    END IF;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Finished generating ' || TO_CHAR(v_inserted, '999,999,999') || ' audit logs.');
END;
/
EXIT;
