-- PL/SQL High-Speed Bulk Inserter for CRM Leads
-- Usage: sqlplus sys/<pwd>@//localhost:1521/FREE as sysdba @generate_crm_data.sql <COUNT> <BATCH_SIZE>
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

  TYPE t_lead_id IS TABLE OF NUMBER;
  TYPE t_contact IS TABLE OF VARCHAR2(100);
  TYPE t_company IS TABLE OF VARCHAR2(100);
  TYPE t_status IS TABLE OF VARCHAR2(30);
  TYPE t_value IS TABLE OF NUMBER(18,2);
  TYPE t_created IS TABLE OF TIMESTAMP;

  v_ids      t_lead_id;
  v_contacts t_contact;
  v_comps    t_company;
  v_statuses t_status;
  v_values   t_value;
  v_dates    t_created;

  TYPE t_stat_list IS VARRAY(7) OF VARCHAR2(30);
  v_status_names t_stat_list := t_stat_list('NEW', 'CONTACTED', 'QUALIFIED', 'PROPOSAL', 'NEGOTIATION', 'CLOSED_WON', 'CLOSED_LOST');
  TYPE t_comp_list IS VARRAY(7) OF VARCHAR2(100);
  v_comp_names t_comp_list := t_comp_list('SCG Corp', 'PTT Group', 'CP All', 'Agoda TH', 'KBank FinTech', 'True Digital', 'Central Retail');
  TYPE t_name_list IS VARRAY(6) OF VARCHAR2(100);
  v_names t_name_list := t_name_list('Sompong T.', 'Nattaporn S.', 'Chatchai M.', 'Karnjana R.', 'Pimchanok K.', 'Thanawat B.');
BEGIN
  SELECT NVL(MAX(LEAD_ID), 0) + 1 INTO v_start_id FROM CRM_APP.LEADS;
  DBMS_OUTPUT.PUT_LINE('Starting bulk generation of ' || TO_CHAR(v_target_count, '999,999,999') || ' leads from LEAD_ID=' || v_start_id);

  WHILE v_inserted < v_target_count LOOP
    v_curr_batch := LEAST(v_batch_size, v_target_count - v_inserted);

    v_ids      := t_lead_id();
    v_contacts := t_contact();
    v_comps    := t_company();
    v_statuses := t_status();
    v_values   := t_value();
    v_dates    := t_created();

    v_ids.EXTEND(v_curr_batch);
    v_contacts.EXTEND(v_curr_batch);
    v_comps.EXTEND(v_curr_batch);
    v_statuses.EXTEND(v_curr_batch);
    v_values.EXTEND(v_curr_batch);
    v_dates.EXTEND(v_curr_batch);

    FOR i IN 1..v_curr_batch LOOP
      v_ids(i)      := v_start_id + v_inserted + (i - 1);
      v_contacts(i) := v_names(TRUNC(DBMS_RANDOM.VALUE(1, 7)));
      v_comps(i)    := v_comp_names(TRUNC(DBMS_RANDOM.VALUE(1, 8)));
      v_statuses(i) := v_status_names(TRUNC(DBMS_RANDOM.VALUE(1, 8)));
      v_values(i)   := ROUND(DBMS_RANDOM.VALUE(10000, 2000000), 2);
      v_dates(i)    := SYSTIMESTAMP;
    END LOOP;

    FORALL i IN 1..v_curr_batch
      INSERT INTO CRM_APP.LEADS (LEAD_ID, CONTACT_NAME, COMPANY, STATUS, ESTIMATED_VALUE, CREATED_AT)
      VALUES (v_ids(i), v_contacts(i), v_comps(i), v_statuses(i), v_values(i), v_dates(i));

    COMMIT;
    v_inserted := v_inserted + v_curr_batch;

    IF MOD(v_inserted, 50000) = 0 OR v_inserted = v_target_count THEN
      DBMS_OUTPUT.PUT_LINE('Inserted: ' || TO_CHAR(v_inserted, '999,999,999') || ' / ' || TO_CHAR(v_target_count, '999,999,999') || ' (' || TO_CHAR(ROUND((v_inserted / v_target_count) * 100, 1)) || '%)');
    END IF;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Finished generating ' || TO_CHAR(v_inserted, '999,999,999') || ' leads.');
END;
/
EXIT;
