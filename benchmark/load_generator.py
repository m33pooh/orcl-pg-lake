"""
High-Performance Load Generator for Oracle 23ai Application Databases
Supports Wallet, Ecommerce, CRM, and Audit applications.
"""

import argparse
import os
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import oracledb

# Database connection profiles for localhost
APP_CONFIGS = {
    "wallet": {
        "dsn": os.getenv("WALLET_DSN", "localhost:11521/FREEPDB1"),
        "user": os.getenv("WALLET_USER", "WALLET_APP"),
        "password": os.getenv("WALLET_PASSWORD", "WalletApp123!"),
        "description": "Wallet App (TRANSACTIONS & WALLET BALANCE UPDATES)",
    },
    "ecommerce": {
        "dsn": os.getenv("ECOMMERCE_DSN", "localhost:1521/FREEPDB1"),
        "user": os.getenv("ECOMMERCE_USER", "ECOMMERCE_APP"),
        "password": os.getenv("ECOMMERCE_PASSWORD", "EcomApp123!"),
        "description": "E-Commerce App (ORDERS)",
    },
    "crm": {
        "dsn": os.getenv("CRM_DSN", "localhost:21521/FREEPDB1"),
        "user": os.getenv("CRM_USER", "CRM_APP"),
        "password": os.getenv("CRM_PASSWORD", "CrmApp123!"),
        "description": "CRM App (LEADS)",
    },
    "audit": {
        "dsn": os.getenv("AUDIT_DSN", "localhost:31521/FREEPDB1"),
        "user": os.getenv("AUDIT_USER", "AUDIT_APP"),
        "password": os.getenv("AUDIT_PASSWORD", "AuditApp123!"),
        "description": "Audit App (SYSTEM_LOGS)",
    },
}


def get_max_id(conn, table_name, id_col):
    with conn.cursor() as cur:
        cur.execute(f"SELECT NVL(MAX({id_col}), 0) FROM {table_name}")
        return cur.fetchone()[0]


def run_wallet_benchmark(count: int, batch_size: int, worker_id: int = 1):
    cfg = APP_CONFIGS["wallet"]
    print(f"[Worker {worker_id}] Connecting to Wallet DB ({cfg['dsn']})...")
    
    with oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"]) as conn:
        with conn.cursor() as cur:
            start_tx_id = get_max_id(conn, "TRANSACTIONS", "TX_ID") + 1
            print(f"[Worker {worker_id}] Starting Wallet insertion from TX_ID={start_tx_id}, total={count:,}")
            
            tx_types = ["DEPOSIT", "WITHDRAW", "TRANSFER_IN", "TRANSFER_OUT", "PAYMENT", "REFUND"]
            wallet_ids = [101, 102, 103, 999]
            
            insert_sql = """
                INSERT INTO TRANSACTIONS (TX_ID, WALLET_ID, AMOUNT, TX_TYPE, CREATED_AT)
                VALUES (:1, :2, :3, :4, :5)
            """
            
            inserted = 0
            start_time = time.time()
            last_report_time = start_time
            last_report_count = 0
            
            while inserted < count:
                curr_batch_size = min(batch_size, count - inserted)
                now = datetime.now()
                batch_data = [
                    (
                        start_tx_id + inserted + i,
                        random.choice(wallet_ids),
                        round(random.uniform(10.0, 5000.0), 2),
                        random.choice(tx_types),
                        now
                    )
                    for i in range(curr_batch_size)
                ]
                
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                inserted += curr_batch_size
                
                curr_time = time.time()
                if curr_time - last_report_time >= 2.0 or inserted >= count:
                    elapsed = curr_time - start_time
                    interval_elapsed = curr_time - last_report_time
                    interval_rate = (inserted - last_report_count) / interval_elapsed if interval_elapsed > 0 else 0
                    overall_rate = inserted / elapsed if elapsed > 0 else 0
                    percent = (inserted / count) * 100
                    print(
                        f"[Wallet] {inserted:,}/{count:,} ({percent:5.1f}%) | "
                        f"Rate: {interval_rate:8.0f} req/s | "
                        f"Avg: {overall_rate:8.0f} req/s | "
                        f"Elapsed: {elapsed:5.1f}s"
                    )
                    last_report_time = curr_time
                    last_report_count = inserted
            
            total_time = time.time() - start_time
            print(f"[Wallet] Finished {inserted:,} requests in {total_time:.2f}s ({inserted/total_time:.0f} req/s)")
            return inserted, total_time


def run_ecommerce_benchmark(count: int, batch_size: int, worker_id: int = 1):
    cfg = APP_CONFIGS["ecommerce"]
    print(f"[Worker {worker_id}] Connecting to Ecommerce DB ({cfg['dsn']})...")
    
    with oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"]) as conn:
        with conn.cursor() as cur:
            start_order_id = get_max_id(conn, "ORDERS", "ORDER_ID") + 1
            print(f"[Worker {worker_id}] Starting Ecommerce insertion from ORDER_ID={start_order_id}, total={count:,}")
            
            statuses = ["PENDING", "PAID", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
            cust_ids = [1, 2]
            
            insert_sql = """
                INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, TOTAL_AMOUNT, STATUS, ORDER_DATE)
                VALUES (:1, :2, :3, :4, :5)
            """
            
            inserted = 0
            start_time = time.time()
            last_report_time = start_time
            last_report_count = 0
            
            while inserted < count:
                curr_batch_size = min(batch_size, count - inserted)
                now = datetime.now()
                batch_data = [
                    (
                        start_order_id + inserted + i,
                        random.choice(cust_ids),
                        round(random.uniform(50.0, 15000.0), 2),
                        random.choice(statuses),
                        now
                    )
                    for i in range(curr_batch_size)
                ]
                
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                inserted += curr_batch_size
                
                curr_time = time.time()
                if curr_time - last_report_time >= 2.0 or inserted >= count:
                    elapsed = curr_time - start_time
                    interval_elapsed = curr_time - last_report_time
                    interval_rate = (inserted - last_report_count) / interval_elapsed if interval_elapsed > 0 else 0
                    overall_rate = inserted / elapsed if elapsed > 0 else 0
                    percent = (inserted / count) * 100
                    print(
                        f"[Ecommerce] {inserted:,}/{count:,} ({percent:5.1f}%) | "
                        f"Rate: {interval_rate:8.0f} req/s | "
                        f"Avg: {overall_rate:8.0f} req/s | "
                        f"Elapsed: {elapsed:5.1f}s"
                    )
                    last_report_time = curr_time
                    last_report_count = inserted
            
            total_time = time.time() - start_time
            print(f"[Ecommerce] Finished {inserted:,} requests in {total_time:.2f}s ({inserted/total_time:.0f} req/s)")
            return inserted, total_time


def run_crm_benchmark(count: int, batch_size: int, worker_id: int = 1):
    cfg = APP_CONFIGS["crm"]
    print(f"[Worker {worker_id}] Connecting to CRM DB ({cfg['dsn']})...")
    
    with oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"]) as conn:
        with conn.cursor() as cur:
            start_lead_id = get_max_id(conn, "LEADS", "LEAD_ID") + 1
            print(f"[Worker {worker_id}] Starting CRM insertion from LEAD_ID={start_lead_id}, total={count:,}")
            
            statuses = ["NEW", "CONTACTED", "QUALIFIED", "PROPOSAL", "NEGOTIATION", "CLOSED_WON", "CLOSED_LOST"]
            companies = ["SCG Corp", "PTT Group", "CP All", "Agoda TH", "KBank FinTech", "True Digital", "Central Retail"]
            names = ["Sompong T.", "Nattaporn S.", "Chatchai M.", "Karnjana R.", "Pimchanok K.", "Thanawat B."]
            
            insert_sql = """
                INSERT INTO LEADS (LEAD_ID, CONTACT_NAME, COMPANY, STATUS, ESTIMATED_VALUE, CREATED_AT)
                VALUES (:1, :2, :3, :4, :5, :6)
            """
            
            inserted = 0
            start_time = time.time()
            last_report_time = start_time
            last_report_count = 0
            
            while inserted < count:
                curr_batch_size = min(batch_size, count - inserted)
                now = datetime.now()
                batch_data = [
                    (
                        start_lead_id + inserted + i,
                        random.choice(names),
                        random.choice(companies),
                        random.choice(statuses),
                        round(random.uniform(10000.0, 2000000.0), 2),
                        now
                    )
                    for i in range(curr_batch_size)
                ]
                
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                inserted += curr_batch_size
                
                curr_time = time.time()
                if curr_time - last_report_time >= 2.0 or inserted >= count:
                    elapsed = curr_time - start_time
                    interval_elapsed = curr_time - last_report_time
                    interval_rate = (inserted - last_report_count) / interval_elapsed if interval_elapsed > 0 else 0
                    overall_rate = inserted / elapsed if elapsed > 0 else 0
                    percent = (inserted / count) * 100
                    print(
                        f"[CRM] {inserted:,}/{count:,} ({percent:5.1f}%) | "
                        f"Rate: {interval_rate:8.0f} req/s | "
                        f"Avg: {overall_rate:8.0f} req/s | "
                        f"Elapsed: {elapsed:5.1f}s"
                    )
                    last_report_time = curr_time
                    last_report_count = inserted
            
            total_time = time.time() - start_time
            print(f"[CRM] Finished {inserted:,} requests in {total_time:.2f}s ({inserted/total_time:.0f} req/s)")
            return inserted, total_time


def run_audit_benchmark(count: int, batch_size: int, worker_id: int = 1):
    cfg = APP_CONFIGS["audit"]
    print(f"[Worker {worker_id}] Connecting to Audit DB ({cfg['dsn']})...")
    
    with oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"]) as conn:
        with conn.cursor() as cur:
            start_log_id = get_max_id(conn, "SYSTEM_LOGS", "LOG_ID") + 1
            print(f"[Worker {worker_id}] Starting Audit insertion from LOG_ID={start_log_id}, total={count:,}")
            
            subsystems = ["AUTH_SVC", "PAYMENT_GW", "ORDER_SVC", "INVENTORY_SVC", "USER_API", "NOTIFICATION_SVC"]
            actions = ["TOKEN_ISSUED", "CHECKOUT_COMPLETED", "INVOICE_GENERATED", "STOCK_RESERVED", "PASSWORD_RESET", "WEBHOOK_DISPATCHED"]
            users = ["admin_sys", "user_1001", "user_1002", "cron_worker", "gateway_agent"]
            severities = ["INFO", "INFO", "INFO", "WARN", "ERROR"]
            
            insert_sql = """
                INSERT INTO SYSTEM_LOGS (LOG_ID, SUBSYSTEM, ACTION, PERFORMED_BY, SEVERITY, LOGGED_AT)
                VALUES (:1, :2, :3, :4, :5, :6)
            """
            
            inserted = 0
            start_time = time.time()
            last_report_time = start_time
            last_report_count = 0
            
            while inserted < count:
                curr_batch_size = min(batch_size, count - inserted)
                now = datetime.now()
                batch_data = [
                    (
                        start_log_id + inserted + i,
                        random.choice(subsystems),
                        random.choice(actions),
                        random.choice(users),
                        random.choice(severities),
                        now
                    )
                    for i in range(curr_batch_size)
                ]
                
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                inserted += curr_batch_size
                
                curr_time = time.time()
                if curr_time - last_report_time >= 2.0 or inserted >= count:
                    elapsed = curr_time - start_time
                    interval_elapsed = curr_time - last_report_time
                    interval_rate = (inserted - last_report_count) / interval_elapsed if interval_elapsed > 0 else 0
                    overall_rate = inserted / elapsed if elapsed > 0 else 0
                    percent = (inserted / count) * 100
                    print(
                        f"[Audit] {inserted:,}/{count:,} ({percent:5.1f}%) | "
                        f"Rate: {interval_rate:8.0f} req/s | "
                        f"Avg: {overall_rate:8.0f} req/s | "
                        f"Elapsed: {elapsed:5.1f}s"
                    )
                    last_report_time = curr_time
                    last_report_count = inserted
            
            total_time = time.time() - start_time
            print(f"[Audit] Finished {inserted:,} requests in {total_time:.2f}s ({inserted/total_time:.0f} req/s)")
            return inserted, total_time


APP_RUNNERS = {
    "wallet": run_wallet_benchmark,
    "ecommerce": run_ecommerce_benchmark,
    "crm": run_crm_benchmark,
    "audit": run_audit_benchmark,
}


def dispatch_runner(app_name: str, count: int, batch_size: int):
    runner = APP_RUNNERS.get(app_name)
    if not runner:
        raise ValueError(f"Unknown app: {app_name}")
    return runner(count, batch_size)


def main():
    parser = argparse.ArgumentParser(description="Oracle 23ai Multi-App Load Generator")
    parser.add_argument(
        "--app",
        choices=["wallet", "ecommerce", "crm", "audit", "all"],
        default="all",
        help="Target application to benchmark (default: all)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1_000_000,
        help="Number of requests/rows to generate per app (default: 1,000,000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5_000,
        help="Batch size for array insert and commit (default: 5,000)",
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("Oracle 23ai Multi-App Load Generator")
    print(f"Target App: {args.app}")
    print(f"Request Count per App: {args.count:,}")
    print(f"Batch Size: {args.batch_size:,}")
    print("=" * 70)
    
    overall_start = time.time()
    
    if args.app == "all":
        apps_to_run = ["wallet", "ecommerce", "crm", "audit"]
        print(f"Executing {len(apps_to_run)} applications concurrently in separate processes...")
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(dispatch_runner, app, args.count, args.batch_size): app
                for app in apps_to_run
            }
            for future in as_completed(futures):
                app = futures[future]
                try:
                    count, duration = future.result()
                    print(f"Summary [{app}]: {count:,} requests in {duration:.2f}s ({count/duration:.0f} req/s)")
                except Exception as e:
                    print(f"Error in {app}: {e}", file=sys.stderr)
    else:
        dispatch_runner(args.app, args.count, args.batch_size)
    
    total_duration = time.time() - overall_start
    total_reqs = args.count * (4 if args.app == "all" else 1)
    avg_throughput = total_reqs / total_duration if total_duration > 0 else 0
    print("=" * 70)
    print(f"All benchmarks finished in {total_duration:.2f} seconds.")
    print(f"Total Requests: {total_reqs:,} | Overall Throughput: {avg_throughput:,.0f} req/s")
    print("=" * 70)


if __name__ == "__main__":
    main()
