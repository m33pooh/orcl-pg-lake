# Enterprise Data Lake Architecture (Debezium Connect - Direct to PostgreSQL)

สถาปัตยกรรมนี้ใช้ **Oracle 23ai Free** จำนวน 4 Instance (แยกตาม Application Domain) เชื่อมต่อผ่าน **Debezium Connect (Kafka Connect Runtime)** ร่วมกับ **Debezium JDBC Sink Connector** เพื่อส่งข้อมูลการเปลี่ยนแปลง (CDC) ไปยัง **PostgreSQL Data Lake** โดยตรงแบบ Real-time โดยไม่ต้องผ่าน n8n เป็นตัวกลาง

สถาปัตยกรรมนี้แบ่งหน้าที่ชัดเจน:
- **CDC Real-Time Data Streaming**: Oracle 23ai -> Debezium Source Connector (LogMiner) -> Kafka -> Debezium JDBC Sink Connector -> PostgreSQL Data Lake
- **Batch ETL & Orchestration**: n8n (แยกอยู่อีกโปรเจกต์ในโฟลเดอร์ `n8n/`) สำหรับดึง Batch Data, Reconciliation, Data Transformation, และสร้าง Analytics Marts

---

## สถาปัตยกรรมระบบ (Architecture Overview)

```
+-----------------------------------------------------------------------------------+
|                              Source Databases (Oracle 23ai)                       |
|   +-------------------+  +-------------------+  +-------------------+  +---------+   |
|   |   oracle-wallet   |  | oracle-ecommerce  |  |    oracle-crm     |  | oracle- |   |
|   |    (WALLET_APP)   |  |  (ECOMMERCE_APP)  |  |     (CRM_APP)     |  |  audit  |   |
+---+---------+---------+--+---------+---------+--+---------+---------+--+----+----+---+
              |                      |                      |                 |
              | LogMiner CDC         | LogMiner CDC         | LogMiner CDC    | LogMiner CDC
              v                      v                      v                 v
+-----------------------------------------------------------------------------------+
|                         Debezium Connect (Kafka Connect)                          |
|   +---------------------------------------------------------------------------+   |
|   | Source Connectors:                                                        |   |
|   | - oracle-wallet-source                                                    |   |
|   | - oracle-ecommerce-source                                                 |   |
|   | - oracle-crm-source                                                       |   |
|   | - oracle-audit-source                                                     |   |
|   +-------------------------------------+-------------------------------------+   |
|                                         | Internal Event Topics                   |
|                                         v                                         |
|   +---------------------------------------------------------------------------+   |
|   | Debezium JDBC Sink Connector:                                             |   |
|   | - pg-datalake-sink                                                        |   |
|   |   - Auto Table Creation (auto.create=true)                                |   |
|   |   - Schema Evolution (auto.evolve=true)                                   |   |
|   |   - Upsert by Primary Key (insert.mode=upsert)                            |   |
|   |   - Delete Propagation (delete.enabled=true)                              |   |
|   +-------------------------------------+-------------------------------------+   |
+-----------------------------------------|-----------------------------------------+
                                          | JDBC Direct Write
                                          v
+-----------------------------------------------------------------------------------+
|                       PostgreSQL 16 Data Lake (postgres-datalake)                 |
|   - wallet_cdc_wallet_app_wallets                                                 |
|   - wallet_cdc_wallet_app_transactions                                            |
|   - ecommerce_cdc_ecommerce_app_customers                                         |
|   - ecommerce_cdc_ecommerce_app_orders                                            |
|   - crm_cdc_crm_app_leads                                                         |
|   - audit_cdc_audit_app_system_logs                                               |
+-----------------------------------------------------------------------------------+
```

---

## ขั้นตอนการเริ่มระบบและตั้งค่า (Setup Guide)

### 1. เริ่มรันคอนเทนเนอร์ด้วย Docker Compose

```bash
cd cdc
docker compose up -d
```

ตรวจสอบสถานะของคอนเทนเนอร์:
```bash
docker compose ps
```

### 2. ตั้งค่า Oracle Database (ARCHIVELOG, Supplemental Logging, User & Compatibility)

รันสคริปต์เปิดโหมด ARCHIVELOG และเตรียมความพร้อมบน Oracle แต่ละโหนด:

```bash
# ตัวอย่างสำหรับ oracle-wallet
docker compose cp setup-cdc-user.sql oracle-wallet:/tmp/setup-cdc-user.sql
docker compose cp setup-wallet-data.sql oracle-wallet:/tmp/setup-wallet-data.sql
docker compose exec oracle-wallet bash -c "export ORACLE_SID=FREE; sqlplus -s / as sysdba @/tmp/setup-cdc-user.sql && sqlplus -s / as sysdba @/tmp/setup-wallet-data.sql"
```

### 3. ลงทะเบียน Connectors ใน Debezium Connect

Debezium Connect ให้บริการ REST API ที่พอร์ต `8083`:

#### 3.1 ลงทะเบียน Debezium JDBC Sink Connector (เขียนลง PostgreSQL)
```bash
curl -X POST -H "Content-Type: application/json" -d @pg-datalake-sink.json http://localhost:8083/connectors
```

#### 3.2 ลงทะเบียน Source Connectors (อ่านจาก Oracle)
```bash
curl -X POST -H "Content-Type: application/json" -d @oracle-wallet-source.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" -d @oracle-ecommerce-source.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" -d @oracle-crm-source.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" -d @oracle-audit-source.json http://localhost:8083/connectors
```

### 4. ตรวจสอบสถานะการทำงาน

ตรวจสอบ Connector ทั้งหมด:
```bash
curl -s http://localhost:8083/connectors
```

ตรวจสอบสถานะ Task ของ Connector:
```bash
curl -s http://localhost:8083/connectors/pg-datalake-sink/status
curl -s http://localhost:8083/connectors/oracle-wallet-source/status
```

ตรวจสอบตารางใน PostgreSQL Data Lake:
```bash
docker compose exec postgres-datalake psql -U datalake_admin -d datalake_db -c "\dt"
```

### 5. Web UI Monitors สำหรับตรวจสอบระบบ

ระบบได้ติดตั้ง Web UI Monitor ไว้พร้อมใช้งาน:
1. **Kafka & Debezium Web UI** (`http://localhost:8080`):
   - ดูรายการ Topics, ข้อความสตรีมมิ่ง JSON, Consumer Group Lags
   - ดูสถานะ Connector ทั้ง 5 ตัว (Source x4, Sink x1) สั่ง Restart/Pause ได้จากหน้าเว็บ
2. **Data Lake Real-Time Replication Dashboard** (`http://localhost:8088`):
   - เปิดดูสรุปจำนวนแถว Oracle เทียบกับ PostgreSQL Data Lake แบบเรียลไทม์
   - คำนวณเปอร์เซ็นต์ความพร้อมและ Replication Lag อัตโนมัติทุก 2 วินาที
```bash
uv run --with oracledb --with "psycopg[binary]" python benchmark/dashboard.py
```

---

## คุณสมบัติสำคัญของ Debezium JDBC Sink

1. **Auto Create & Auto Evolve Table**:
   เมื่อ Debezium Source เริ่มอ่านตารางใหม่ หรือมีการเพิ่มฟิลด์ใหม่ JDBC Sink Connector จะสร้างตารางและปรับปรุงโครงสร้างตาราง (Column DDL) บน PostgreSQL ให้โดยอัตโนมัติ

2. **Upsert by Record Key**:
   รองรับการ Insert หรือ Update ทับตาม Primary Key โดยไม่มีปัญหา Duplicate Key Error

3. **Delete Propagation**:
   เมื่อมีการ Delete แถวข้อมูลใน Oracle ตัว Sink Connector จะส่งคำสั่ง `DELETE` ไปยัง PostgreSQL ตาม Primary Key อย่างสมบูรณ์