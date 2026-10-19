-- PL/SQL High-Speed Bulk Inserter for Wallet Transactions
-- Usage: sqlplus sys/<pwd>@//localhost:1521/FREE as sysdba @generate_wallet_data.sql <COUNT> <BATCH_SIZE>
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

  TYPE t_tx_id IS TABLE OF NUMBER;
  TYPE t_wallet_id IS TABLE OF NUMBER;
  TYPE t_amount IS TABLE OF NUMBER(18,2);
  TYPE t_tx_type IS TABLE OF VARCHAR2(20);
  TYPE t_created_at IS TABLE OF TIMESTAMP;

  v_ids         t_tx_id;
  v_wids        t_wallet_id;
  v_amts        t_amount;
  v_types       t_tx_type;
  v_dates       t_created_at;

  TYPE t_type_list IS VARRAY(6) OF VARCHAR2(20);
  v_type_names t_type_list := t_type_list('DEPOSIT', 'WITHDRAW', 'TRANSFER_IN', 'TRANSFER_OUT', 'PAYMENT', 'REFUND');
  TYPE t_wid_list IS VARRAY(4) OF NUMBER;
  v_valid_wids t_wid_list := t_wid_list(101, 102, 103, 999);
BEGIN
  SELECT NVL(MAX(TX_ID), 0) + 1 INTO v_start_id FROM WALLET_APP.TRANSACTIONS;
  DBMS_OUTPUT.PUT_LINE('Starting bulk generation of ' || TO_CHAR(v_target_count, '999,999,999') || ' transactions from TX_ID=' || v_start_id);

  WHILE v_inserted < v_target_count LOOP
    v_curr_batch := LEAST(v_batch_size, v_target_count - v_inserted);

    v_ids   := t_tx_id();
    v_wids  := t_wallet_id();
    v_amts  := t_amount();
    v_types := t_tx_type();
    v_dates := t_created_at();

    v_ids.EXTEND(v_curr_batch);
    v_wids.EXTEND(v_curr_batch);
    v_amts.EXTEND(v_curr_batch);
    v_types.EXTEND(v_curr_batch);
    v_dates.EXTEND(v_curr_batch);

    FOR i IN 1..v_curr_batch LOOP
      v_ids(i)   := v_start_id + v_inserted + (i - 1);
      v_wids(i)  := v_valid_wids(TRUNC(DBMS_RANDOM.VALUE(1, 5)));
      v_amts(i)  := ROUND(DBMS_RANDOM.VALUE(10, 5000), 2);
      v_types(i) := v_type_names(TRUNC(DBMS_RANDOM.VALUE(1, 7)));
      v_dates(i) := SYSTIMESTAMP;
    END LOOP;

    FORALL i IN 1..v_curr_batch
      INSERT INTO WALLET_APP.TRANSACTIONS (TX_ID, WALLET_ID, AMOUNT, TX_TYPE, CREATED_AT)
      VALUES (v_ids(i), v_wids(i), v_amts(i), v_types(i), v_dates(i));

    COMMIT;
    v_inserted := v_inserted + v_curr_batch;

    IF MOD(v_inserted, 50000) = 0 OR v_inserted = v_target_count THEN
      DBMS_OUTPUT.PUT_LINE('Inserted: ' || TO_CHAR(v_inserted, '999,999,999') || ' / ' || TO_CHAR(v_target_count, '999,999,999') || ' (' || TO_CHAR(ROUND((v_inserted / v_target_count) * 100, 1)) || '%)');
    END IF;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Finished generating ' || TO_CHAR(v_inserted, '999,999,999') || ' transactions.');
END;
/
EXIT;
