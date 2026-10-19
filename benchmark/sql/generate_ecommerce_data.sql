-- PL/SQL High-Speed Bulk Inserter for Ecommerce Orders
-- Usage: sqlplus sys/<pwd>@//localhost:1521/FREE as sysdba @generate_ecommerce_data.sql <COUNT> <BATCH_SIZE>
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

  TYPE t_order_id IS TABLE OF NUMBER;
  TYPE t_cust_id IS TABLE OF NUMBER;
  TYPE t_total_amt IS TABLE OF NUMBER(18,2);
  TYPE t_status IS TABLE OF VARCHAR2(20);
  TYPE t_order_date IS TABLE OF TIMESTAMP;

  v_ids      t_order_id;
  v_cids     t_cust_id;
  v_amts     t_total_amt;
  v_statuses t_status;
  v_dates    t_order_date;

  TYPE t_stat_list IS VARRAY(6) OF VARCHAR2(20);
  v_status_names t_stat_list := t_stat_list('PENDING', 'PAID', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED');
  TYPE t_cid_list IS VARRAY(2) OF NUMBER;
  v_valid_cids t_cid_list := t_cid_list(1, 2);
BEGIN
  SELECT NVL(MAX(ORDER_ID), 0) + 1 INTO v_start_id FROM ECOMMERCE_APP.ORDERS;
  DBMS_OUTPUT.PUT_LINE('Starting bulk generation of ' || TO_CHAR(v_target_count, '999,999,999') || ' orders from ORDER_ID=' || v_start_id);

  WHILE v_inserted < v_target_count LOOP
    v_curr_batch := LEAST(v_batch_size, v_target_count - v_inserted);

    v_ids      := t_order_id();
    v_cids     := t_cust_id();
    v_amts     := t_total_amt();
    v_statuses := t_status();
    v_dates    := t_order_date();

    v_ids.EXTEND(v_curr_batch);
    v_cids.EXTEND(v_curr_batch);
    v_amts.EXTEND(v_curr_batch);
    v_statuses.EXTEND(v_curr_batch);
    v_dates.EXTEND(v_curr_batch);

    FOR i IN 1..v_curr_batch LOOP
      v_ids(i)      := v_start_id + v_inserted + (i - 1);
      v_cids(i)     := v_valid_cids(TRUNC(DBMS_RANDOM.VALUE(1, 3)));
      v_amts(i)     := ROUND(DBMS_RANDOM.VALUE(50, 15000), 2);
      v_statuses(i) := v_status_names(TRUNC(DBMS_RANDOM.VALUE(1, 7)));
      v_dates(i)    := SYSTIMESTAMP;
    END LOOP;

    FORALL i IN 1..v_curr_batch
      INSERT INTO ECOMMERCE_APP.ORDERS (ORDER_ID, CUSTOMER_ID, TOTAL_AMOUNT, STATUS, ORDER_DATE)
      VALUES (v_ids(i), v_cids(i), v_amts(i), v_statuses(i), v_dates(i));

    COMMIT;
    v_inserted := v_inserted + v_curr_batch;

    IF MOD(v_inserted, 50000) = 0 OR v_inserted = v_target_count THEN
      DBMS_OUTPUT.PUT_LINE('Inserted: ' || TO_CHAR(v_inserted, '999,999,999') || ' / ' || TO_CHAR(v_target_count, '999,999,999') || ' (' || TO_CHAR(ROUND((v_inserted / v_target_count) * 100, 1)) || '%)');
    END IF;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Finished generating ' || TO_CHAR(v_inserted, '999,999,999') || ' orders.');
END;
/
EXIT;
