"""
High-Performance Lake Ingestion Receiver for Standalone Debezium Server
Receives HTTP CDC batches from Debezium Server and writes multi-values bulk upserts into PostgreSQL Data Lake.
"""

import asyncio
import os
import sys
import time
from typing import Dict, List, Any

import asyncpg
import orjson
import grpc
from fastapi import FastAPI, Request, Response, status

import cdc_pb2
import cdc_pb2_grpc

PG_HOST = os.getenv("PG_HOST", "postgres-datalake")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "datalake_admin")
PG_PWD = os.getenv("PG_PWD", "DataLakePwd123!")
PG_DB = os.getenv("PG_DB", "datalake_db")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
FLUSH_INTERVAL = float(os.getenv("FLUSH_INTERVAL", "0.05"))

# Schema definition and PK mapping
TABLE_CONFIGS = {
    "TRANSACTIONS": {
        "pg_table": "wallet_cdc_wallet_app_transactions",
        "pk": "tx_id",
        "mode": "staging_merge",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS wallet_cdc_wallet_app_transactions (
                tx_id BIGINT PRIMARY KEY,
                wallet_id BIGINT,
                amount NUMERIC,
                tx_type VARCHAR(50),
                created_at TIMESTAMPTZ
            );
        """
    },
    "WALLETS": {
        "pg_table": "wallet_cdc_wallet_app_wallets",
        "pk": "wallet_id",
        "mode": "staging_merge",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS wallet_cdc_wallet_app_wallets (
                wallet_id BIGINT PRIMARY KEY,
                user_id BIGINT,
                balance NUMERIC,
                currency VARCHAR(20),
                status VARCHAR(50),
                updated_at TIMESTAMPTZ
            );
        """
    },
    "CUSTOMERS": {
        "pg_table": "ecommerce_cdc_ecommerce_app_customers",
        "pk": "customer_id",
        "mode": "staging_merge",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS ecommerce_cdc_ecommerce_app_customers (
                customer_id BIGINT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                tier VARCHAR(50),
                created_at TIMESTAMPTZ
            );
        """
    },
    "ORDERS": {
        "pg_table": "ecommerce_cdc_ecommerce_app_orders",
        "pk": "order_id",
        "mode": "staging_merge",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS ecommerce_cdc_ecommerce_app_orders (
                order_id BIGINT PRIMARY KEY,
                customer_id BIGINT,
                total_amount NUMERIC,
                status VARCHAR(50),
                order_date TIMESTAMPTZ
            );
        """
    },
    "LEADS": {
        "pg_table": "crm_cdc_crm_app_leads",
        "pk": "lead_id",
        "mode": "staging_merge",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS crm_cdc_crm_app_leads (
                lead_id BIGINT PRIMARY KEY,
                contact_name VARCHAR(255),
                company VARCHAR(255),
                status VARCHAR(50),
                estimated_value NUMERIC,
                created_at TIMESTAMPTZ
            );
        """
    },
    "SYSTEM_LOGS": {
        "pg_table": "audit_cdc_audit_app_system_logs",
        "pk": "log_id",
        "mode": "copy_direct",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS audit_cdc_audit_app_system_logs (
                log_id BIGINT PRIMARY KEY,
                subsystem VARCHAR(100),
                action VARCHAR(100),
                performed_by VARCHAR(100),
                severity VARCHAR(50),
                logged_at TIMESTAMPTZ
            );
        """
    }
}

app = FastAPI(title="Lake Receiver", version="2.0.0")

# In-memory queues and stats
queues: Dict[str, asyncio.Queue] = {}
stats = {
    "total_received": 0,
    "total_upserted": 0,
    "start_time": time.time(),
    "by_table": {}
}

pool: asyncpg.Pool = None


async def get_pg_pool():
    global pool
    for i in range(30):
        try:
            pool = await asyncpg.create_pool(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PWD,
                database=PG_DB,
                min_size=5,
                max_size=20
            )
            print("Connected to PostgreSQL Data Lake pool.")
            break
        except Exception as e:
            print(f"Waiting for PostgreSQL Data Lake: {e}")
            await asyncio.sleep(2)
    return pool


class LakeIngestionServicer(cdc_pb2_grpc.IngestionServiceServicer):
    async def SendBatch(self, request, context):
        app_name = request.app_name
        records = request.records
        processed = 0

        for r in records:
            if not r.value:
                continue
            try:
                data = orjson.loads(r.value)
            except Exception:
                continue

            payload = data.get("payload", data)
            source = payload.get("source", {})
            t_name = source.get("table", "").upper()
            after_record = payload.get("after")

            if after_record and t_name in TABLE_CONFIGS:
                norm_row = {k.lower(): v for k, v in after_record.items()}
                cleaned = clean_row(norm_row)
                q = queues.get(t_name)
                if q:
                    await q.put(cleaned)
                    stats["total_received"] += 1
                    stats["by_table"][t_name]["received"] += 1
                    processed += 1

        return cdc_pb2.BatchResponse(success=True, processed_count=processed)


async def run_grpc_server():
    server = grpc.aio.server(
        options=[
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ('grpc.max_send_message_length', 100 * 1024 * 1024),
        ]
    )
    cdc_pb2_grpc.add_IngestionServiceServicer_to_server(LakeIngestionServicer(), server)
    server.add_insecure_port('0.0.0.0:50051')
    await server.start()
    print("gRPC Ingestion Server listening on port 50051.")
    await server.wait_for_termination()


@app.on_event("startup")
async def startup_event():
    await get_pg_pool()
    
    # Initialize tables and queues
    async with pool.acquire() as conn:
        for t_name, cfg in TABLE_CONFIGS.items():
            await conn.execute(cfg["create_sql"])
            queues[t_name] = asyncio.Queue(maxsize=100000)
            stats["by_table"][t_name] = {"received": 0, "upserted": 0}
            # Start background flusher for each table
            asyncio.create_task(table_flush_worker(t_name, cfg))
    print("All table background workers initialized.")
    # Start high-performance gRPC Server on 50051
    asyncio.create_task(run_grpc_server())


@app.on_event("shutdown")
async def shutdown_event():
    if pool:
        await pool.close()


async def table_flush_worker(table_name: str, cfg: Dict[str, Any]):
    """Background worker draining queue and executing high-throughput ingestion."""
    q = queues[table_name]
    pg_table = cfg["pg_table"]
    pk = cfg["pk"]
    mode = cfg.get("mode", "staging_merge")
    
    while True:
        items = []
        try:
            # Wait for first item
            item = await q.get()
            items.append(item)
            q.task_done()
            
            # Drain up to BATCH_SIZE items or until empty
            while len(items) < BATCH_SIZE and not q.empty():
                it = q.get_nowait()
                items.append(it)
                q.task_done()
        except asyncio.CancelledError:
            break
        except Exception:
            pass

        if not items:
            await asyncio.sleep(FLUSH_INTERVAL)
            continue

        # Execute high-performance hybrid ingestion
        try:
            await execute_hybrid_ingest(pg_table, pk, items, mode)
            stats["total_upserted"] += len(items)
            stats["by_table"][table_name]["upserted"] += len(items)
        except Exception as e:
            print(f"Error ingesting into {pg_table}: {e}", file=sys.stderr)


async def execute_hybrid_ingest(pg_table: str, pk_col: str, rows: List[Dict[str, Any]], mode: str = "staging_merge"):
    """
    High-Performance PostgreSQL Ingestion Engine:
    - 'copy_direct': Uses native asyncpg binary COPY protocol (150,000 - 200,000+ row/s)
    - 'staging_merge': Uses binary COPY into temporary staging table + Set-Based bulk MERGE (100,000+ row/s)
    """
    if not rows:
        return

    first_row = rows[0]
    cols = list(first_row.keys())
    records = [tuple(r.get(c) for c in cols) for r in rows]

    # Mode 1: Pure Append-Only Binary COPY
    if mode == "copy_direct":
        try:
            async with pool.acquire() as conn:
                await conn.copy_records_to_table(
                    table_name=pg_table,
                    records=records,
                    columns=cols
                )
            return
        except asyncpg.exceptions.UniqueViolationError:
            # Fallback to staging merge if duplicate key occurs
            pass

    # Mode 2: Staging Table Binary COPY + Set-Based Bulk Merge
    col_str = ", ".join([f'"{c}"' for c in cols])
    update_cols = [c for c in cols if c != pk_col]
    if update_cols:
        update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        conflict_clause = f'ON CONFLICT ("{pk_col}") DO UPDATE SET {update_clause}'
    else:
        conflict_clause = f'ON CONFLICT ("{pk_col}") DO NOTHING'

    stg_table = f"stg_{pg_table}"
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f'CREATE TEMP TABLE "{stg_table}" (LIKE "{pg_table}") ON COMMIT DROP;')
            await conn.copy_records_to_table(
                table_name=stg_table,
                records=records,
                columns=cols
            )
            await conn.execute(f"""
                INSERT INTO "{pg_table}" ({col_str})
                SELECT DISTINCT ON ("{pk_col}") {col_str}
                FROM "{stg_table}"
                ORDER BY "{pk_col}"
                {conflict_clause};
            """)


from decimal import Decimal
import base64
from datetime import datetime


def decode_val(k, v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if k in ("amount", "balance", "total_amount", "estimated_value"):
            return Decimal(str(round(v, 2)))
        return v
    if isinstance(v, dict) and "scale" in v and "value" in v:
        try:
            raw = base64.b64decode(v["value"])
            int_val = int.from_bytes(raw, byteorder="big", signed=True)
            scale = v.get("scale", 0)
            res = int_val / (10 ** scale) if scale > 0 else int_val
            return Decimal(str(round(res, scale if scale > 0 else 2)))
        except Exception:
            return None
    if k in ("amount", "balance", "total_amount", "estimated_value"):
        if isinstance(v, str):
            try:
                return Decimal(str(round(float(v), 2)))
            except ValueError:
                pass
            try:
                raw = base64.b64decode(v)
                int_val = int.from_bytes(raw, byteorder="big", signed=True)
                return Decimal(str(round(int_val / 100.0, 2)))
            except Exception:
                return None
    return v


def clean_row(row: dict):
    cleaned = {}
    for k, v in row.items():
        val = decode_val(k, v)
        if k in ("created_at", "updated_at", "order_date", "logged_at"):
            if isinstance(val, (int, float)):
                if val > 1e14:
                    cleaned[k] = datetime.utcfromtimestamp(val / 1e6)
                elif val > 1e11:
                    cleaned[k] = datetime.utcfromtimestamp(val / 1e3)
                else:
                    cleaned[k] = datetime.utcfromtimestamp(val)
            elif isinstance(val, str):
                try:
                    cleaned[k] = datetime.fromisoformat(val.replace("Z", "")).replace(tzinfo=None)
                except Exception:
                    cleaned[k] = None
            elif isinstance(val, datetime):
                cleaned[k] = val.replace(tzinfo=None)
            else:
                cleaned[k] = val
        else:
            cleaned[k] = val
    return cleaned


@app.post("/webhook/{app_name}")
async def receive_cdc_event(app_name: str, request: Request):
    """Receives Debezium HTTP Change Event and pushes to table queue."""
    raw_body = await request.body()
    if not raw_body:
        return Response(status_code=status.HTTP_200_OK)

    try:
        data = orjson.loads(raw_body)
    except Exception:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)

    payload = data.get("payload", data)
    source = payload.get("source", {})
    t_name = source.get("table", "").upper()

    after_record = payload.get("after")
    if after_record and t_name in TABLE_CONFIGS:
        norm_row = {k.lower(): v for k, v in after_record.items()}
        cleaned = clean_row(norm_row)
        q = queues.get(t_name)
        if q:
            await q.put(cleaned)
            stats["total_received"] += 1
            stats["by_table"][t_name]["received"] += 1

    return Response(status_code=status.HTTP_200_OK)


@app.get("/health")
async def health():
    return {"status": "ok", "pool_connected": pool is not None}


@app.get("/stats")
async def get_stats():
    now = time.time()
    uptime = now - stats["start_time"]
    rate = stats["total_upserted"] / uptime if uptime > 0 else 0
    return {
        "uptime_seconds": round(uptime, 2),
        "total_received": stats["total_received"],
        "total_upserted": stats["total_upserted"],
        "throughput_req_per_sec": round(rate, 2),
        "by_table": stats["by_table"]
    }
