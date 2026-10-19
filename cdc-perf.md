# CDC Performance Benchmark Report (Option B2: Direct Custom gRPC Stream + Hybrid Binary COPY)

รายงานสรุปผลการทดสอบประสิทธิภาพระบบ Change Data Capture (CDC) จาก **Oracle 23ai Free (4 Databases)** สู่ **PostgreSQL 16 Enterprise Data Lake** แบบ **Zero-Kafka Standalone Runtime** ผ่านสถาปัตยกรรม **Option B2: Direct Custom gRPC Stream with Protobuf Batching** และระบบ Ingestion แบบ **Hybrid Binary COPY Protocol**

---

## 1. ข้อมูลสรุปภาพรวม (Executive Summary)

| มิติการวัดผล | รายละเอียดสถาปัตยกรรมและผลลัพธ์ที่วัดได้จริง |
| :--- | :--- |
| **Data Ingestion Engine** | Hybrid Binary Ingestion Engine (`conn.copy_records_to_table` + Staging Table Merge) |
| **Transport Layer** | High-Performance gRPC (HTTP/2 Multiplexing) ผ่านพอร์ต 50051 |
| **CDC Source Connector** | Debezium Server 2.4 (Quarkus 3 Standalone Runtime ไร้ Kafka) |
| **จำนวนข้อมูลทดสอบทั้งหมด** | **1,000,000 รายการ / แอปพลิเคชัน (รวม 4,000,000 รายการ)** |
| **Oracle Source Throughput** | **175,393 row/s** (สร้างครบ 4,000,000 แถว ใน 22.81 วินาที) |
| **Peak Ingestion Throughput** | **20,567.79 row/s** |
| **Average End-to-End Throughput** | **19,527.60 row/s** |
| **เวลาในการซิงค์จนจบ (Zero Lag)** | **204.84 วินาที (~3.4 นาที)** สำหรับ 4,000,000 รายการ |
| **ความถูกต้องของข้อมูล (Completeness)** | **100.00% Sync (Data Lag = 0 ทุกแอปพลิเคชัน)** |

---

## 2. รายละเอียดตาราง จำนวนฟิลด์ และขนาดข้อมูล (Schema & Byte/Row)

วัดค่าจริงจาก PostgreSQL 16 Data Lake (`datalake_db`) โดยใช้ `avg(pg_column_size(t.*))` สำหรับขนาดข้อมูลดิบ (Raw Data Payload) และ `pg_relation_size` สำหรับขนาดพื้นที่จัดเก็บบนดิสก์

| Application | Data Lake Table Name | จำนวนฟิลด์ (Columns) | รายชื่อและประเภทฟิลด์ | Ingest Mode | Raw Data (byte/row) | Disk Storage (byte/row) | รวม Index (byte/row) |
| :--- | :--- | :---: | :--- | :--- | :---: | :---: | :---: |
| **Wallet App** | `wallet_cdc_wallet_app_transactions` | **5** | `tx_id`, `wallet_id`, `amount`, `tx_type`, `created_at` | Staging Table Merge | **66.67** | **71.27** | **94.29** |
| **E-Commerce App** | `ecommerce_cdc_ecommerce_app_orders` | **5** | `order_id`, `customer_id`, `total_amount`, `status`, `order_date` | Staging Table Merge | **68.84** | **73.40** | **96.42** |
| **CRM App** | `crm_cdc_crm_app_leads` | **6** | `lead_id`, `contact_name`, `company`, `status`, `estimated_value`, `created_at` | Staging Table Merge | **85.04** | **89.95** | **112.97** |
| **Audit App** | `audit_cdc_audit_app_system_logs` | **6** | `log_id`, `subsystem`, `action`, `performed_by`, `severity`, `logged_at` | Direct Binary COPY | **87.94** | **92.82** | **115.83** |
| **เฉลี่ยรวม** | **All 4 Data Lake Tables** | **5.5** | - | - | **77.12** | **81.86** | **104.88** |

### คำอธิบายขนาดข้อมูล (Payload Sizing)
- **Raw Data (byte/row)**: ปริมาณข้อมูลจริงของแต่ละเรคอร์ดในหน่วยความจำ ไม่รวมโครงสร้าง Tuple Header
- **Disk Storage (byte/row)**: ขนาดพื้นที่ใน Table Heap ต่อแถว รวม Block Overhead และ Tuple Header (23 bytes)
- **รวม Index (byte/row)**: พื้นที่จัดเก็บจริงทั้งหมดบนดิสก์รวม B-Tree Primary Key Index
- **สเกลพื้นที่จัดเก็บข้อมูลสำหรับ 4,000,000 แถว**:
  - Raw Data Payload รวม: **~308 MB**
  - Table Heap บนดิสก์: **~327 MB**
  - B-Tree Primary Key Indexes บนดิสก์: **~92 MB**
  - **รวมพื้นที่บน PostgreSQL Data Lake**: **~420 MB**

---

## 3. ผลการทดสอบรอบที่ 1 Baseline: 100,000 รายการต่อแอป (รวม 400,000 รายการ)

### 3.1 ความเร็วฝั่งต้นทาง (Oracle Source Insertion)
รันแบบขนาน 4 โปรเซสผ่าน `benchmark/load_generator.py` ด้วย Array Binding (`executemany`, batch size = 5,000):

| Application | จำนวนคำขอ (Rows) | เวลาที่ใช้ (วินาที) | Throughput ฝั่งต้นทาง (row/s) |
| :--- | :---: | :---: | :---: |
| **Audit App** | 100,000 | 1.05s | **95,555 row/s** |
| **E-Commerce App** | 100,000 | 1.24s | **80,436 row/s** |
| **Wallet App** | 100,000 | 1.28s | **77,995 row/s** |
| **CRM App** | 100,000 | 1.31s | **76,136 row/s** |
| **Total (Concurrent)** | **400,000** | **1.69s** | **236,187 row/s** |

### 3.2 ความเร็วการสตรีมและสถานะความสมบูรณ์ (End-to-End Ingestion)
- **Peak Throughput**: **11,181.48 row/s**
- **เวลาที่ใช้จนจบ (Zero Lag)**: **35.77 วินาที**
- **ผลลัพธ์ Data Lag**: **0** ทุกแอปพลิเคชัน (100.00% ซิงค์สมบูรณ์)

---

## 4. ผลการทดสอบรอบใหม่: 1,000,000 รายการต่อแอป (รวม 4,000,000 รายการ)

### 4.1 ความเร็วฝั่งต้นทาง (Oracle Source Insertion 4,000,000 Rows)
รันขนาน 4 โปรเซสผ่าน `benchmark/load_generator.py` ด้วย Batch Size = 10,000:

| Application | Target Oracle Table | จำนวนคำขอ (Rows) | เวลาที่ใช้ (วินาที) | Throughput ฝั่งต้นทาง (row/s) |
| :--- | :--- | :---: | :---: | :---: |
| **CRM App** | `FREEPDB1.CRM_APP.LEADS` | 1,000,000 | 21.59s | **46,311 row/s** |
| **E-Commerce App** | `FREEPDB1.ECOMMERCE_APP.ORDERS` | 1,000,000 | 21.91s | **45,649 row/s** |
| **Audit App** | `FREEPDB1.AUDIT_APP.SYSTEM_LOGS` | 1,000,000 | 22.10s | **45,257 row/s** |
| **Wallet App** | `FREEPDB1.WALLET_APP.TRANSACTIONS` | 1,000,000 | 22.42s | **44,597 row/s** |
| **Total (Concurrent)** | **All 4 Applications** | **4,000,000** | **22.81s** | **175,393 row/s** |

### 4.2 ความเร็วการสตรีม CDC ผ่าน Option B2 gRPC + Hybrid Binary COPY
Debezium Server ดึง Redo Log สตรีมผ่าน gRPC HTTP/2 -> Lake Receiver บันทึกลง PostgreSQL Data Lake ด้วย Direct Binary COPY และ Staging Table Merge:

| มิติการวัดผล | ผลลัพธ์ที่วัดได้จริง | หมายเหตุ |
| :--- | :---: | :--- |
| **Peak Throughput** | **20,567.79 row/s** | จุดสูงสุดขณะประมวลผลทั้ง 4 Streams พร้อมกัน |
| **Average End-to-End Throughput** | **19,527.60 row/s** | อัตราเฉลี่ยตลอดระยะเวลาการทำงาน |
| **เวลาที่ใช้จนกระทั่ง Zero Lag** | **204.84 วินาที (~3.4 นาที)** | บันทึกครบ 4,000,000 รายการลงสู่ Data Lake |
| **ความสมบูรณ์ของข้อมูล (Completeness)** | **100.00%** | ข้อมูลเท่ากัน 4,000,000 จาก 4,000,000 รายการ |
| **Data Lag คงค้าง** | **0** | ไม่มีข้อมูลค้างท่อในระบบ |

### 4.3 สถานะความสมบูรณ์แยกรายแอปพลิเคชัน (Final Sync Table)

| Application | ต้นทาง Oracle 23ai | ปลายทาง PostgreSQL | คงค้าง (Data Lag) | สถานะความพร้อม (Sync %) | รูปแบบ Ingestion |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Wallet App** | 1,000,000 | 1,000,000 | **0** | **100.0%** | Staging Table Merge |
| **E-Commerce App** | 1,000,000 | 1,000,000 | **0** | **100.0%** | Staging Table Merge |
| **CRM App** | 1,000,000 | 1,000,000 | **0** | **100.0%** | Staging Table Merge |
| **Audit App** | 1,000,000 | 1,000,000 | **0** | **100.0%** | Direct Binary COPY |
| **สรุปภาพรวม** | **4,000,000** | **4,000,000** | **0** | **100.0%** | **Peak: 20,567.79 row/s** |

---

## 5. เปรียบเทียบประสิทธิภาพ: HTTP/1.1 Webhook เดิม vs Option B2 gRPC Stream + Hybrid Binary COPY

| มิติการเปรียบเทียบ | HTTP/1.1 REST Webhooks (เดิม) | Option B2 Custom gRPC (เดิม) | Option B2 + Hybrid Binary COPY (ปัจจุบัน) |
| :--- | :--- | :--- | :--- |
| **Transport Layer** | HTTP/1.1 (Single Request-Response) | HTTP/2 (Multiplexed Channel) | HTTP/2 (Multiplexed Channel) |
| **Serialization Format** | Plain Text JSON String | Binary Protocol Buffers | Binary Protocol Buffers |
| **PostgreSQL Write Protocol** | Parameterized Multi-Row INSERT | Parameterized Multi-Row INSERT | **Binary COPY Protocol (Direct & Staging Merge)** |
| **PostgreSQL Write Speed** | ~11,000 row/s | ~16,000 row/s | **114,000 - 189,000 row/s** |
| **Kafka Dependency** | ไม่มี (Zero-Kafka) | ไม่มี (Zero-Kafka) | ไม่มี (Zero-Kafka Standalone Runtime) |

---

## 6. โครงสร้างไฟล์โค้ดและมอนิเตอร์ที่เกี่ยวข้อง
- **gRPC Contract Definition**: [cdc/proto/cdc.proto](file:///c:/Users/asuslap/git/orcl-pg-lake/cdc/proto/cdc.proto)
- **Custom Quarkus gRPC Client**: [cdc/grpc-test/src/main/java/io/debezium/server/http/HttpChangeConsumer.java](file:///c:/Users/asuslap/git/orcl-pg-lake/cdc/grpc-test/src/main/java/io/debezium/server/http/HttpChangeConsumer.java)
- **Hybrid Binary COPY Ingestion Server**: [cdc/receiver/main.py](file:///c:/Users/asuslap/git/orcl-pg-lake/cdc/receiver/main.py)
- **Load Generation & Benchmark Suite**: [benchmark/load_generator.py](file:///c:/Users/asuslap/git/orcl-pg-lake/benchmark/load_generator.py)
- **Live Monitoring Dashboard**: [benchmark/dashboard.py](file:///c:/Users/asuslap/git/orcl-pg-lake/benchmark/dashboard.py) ([http://localhost:8088](http://localhost:8088))
- **Live Receiver Stats**: [http://localhost:8000/stats](http://localhost:8000/stats)
