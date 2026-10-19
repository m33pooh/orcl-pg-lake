# 🚀 Enterprise Data Lake Architecture (CDC with Oracle 23ai & PostgreSQL)

สถาปัตยกรรมนี้ใช้ **Oracle 23ai Free** จำนวน 4 ตัว (แยกตามแอปพลิเคชันเพื่อรับโควตา 12 GB ต่อแอป) และใช้ **PostgreSQL** เป็น Data Lake โดยมี **Kafka & Debezium** ทำหน้าที่ดูดข้อมูลแบบ Real-time (Change Data Capture - CDC)

*(หมายเหตุ: โปรเจกต์นี้จำเป็นต้องรันร่วมกับไฟล์ `docker-compose.yml` ที่ได้เตรียมไว้)*

---

## 🛠️ วิธีการรันระบบและตั้งค่า CDC

### ขั้นตอนที่ 1: Start Services
เปิด Terminal ในโฟลเดอร์ที่มีไฟล์ `docker-compose.yml` แล้วรันคำสั่ง:
```bash
docker-compose up -d
```
*(รอประมาณ 3-5 นาทีเพื่อให้ Oracle Database ทั้ง 4 ตัว และ Service อื่นๆ Start จนเสร็จสมบูรณ์)*

---

### ขั้นตอนที่ 2: เปิดโหมด Archive Log บน Oracle
เพื่อให้ Debezium สามารถอ่าน Log การเปลี่ยนแปลงข้อมูลได้ คุณต้องเปิดฟีเจอร์นี้บน Oracle **ทั้ง 4 ตัว** (ตัวอย่างด้านล่างคือการทำกับ `oracle-wallet`)

1. เข้าไปใน Container ของ Oracle:
   ```bash
   docker exec -it oracle-wallet bash
   ```
2. ล็อกอินเข้า SQL*Plus ด้วยสิทธิ์ผู้ดูแลระบบ:
   ```bash
   sqlplus / as sysdba
   ```
3. รันคำสั่ง SQL ต่อไปนี้เพื่อเปิด LogMiner:
   ```sql
   SHUTDOWN IMMEDIATE;
   STARTUP MOUNT;
   ALTER DATABASE ARCHIVELOG;
   ALTER DATABASE OPEN;
   
   -- เปิด Supplemental Logging เพื่อให้ Log เก็บข้อมูลครบถ้วน
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
   ```

---

### ขั้นตอนที่ 3: สร้าง User สำหรับ Debezium (CDC User)
ในหน้าจอ SQL*Plus เดิม ให้รันคำสั่งชุดนี้เพื่อสร้าง User ให้ Debezium มีสิทธิ์เข้าไปอ่าน Log การทำธุรกรรม:

```sql
CREATE USER c##dbzuser IDENTIFIED BY dbz_password CONTAINER=ALL;
GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
```

*(ทำขั้นตอนที่ 2 และ 3 ซ้ำกับคอนเทนเนอร์ `oracle-ecommerce`, `oracle-crm`, และ `oracle-audit`)*

---

### ขั้นตอนที่ 4: สั่งให้ Debezium เริ่มดูดข้อมูล
เมื่อตั้งค่า Database เสร็จสิ้นแล้ว ให้เปิด Terminal หน้าต่างใหม่ แล้วยิง HTTP POST Request ไปที่ API ของ Debezium (พอร์ต 8083) เพื่อสร้าง Connector สั่งให้เริ่มดูดข้อมูล (ตัวอย่างนี้คือการดูดจาก Wallet App):

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
localhost:8083/connectors/ -d '{
  "name": "wallet-connector",
  "config": {
    "connector.class" : "io.debezium.connector.oracle.OracleConnector",
    "tasks.max" : "1",
    "database.hostname" : "oracle-wallet",
    "database.port" : "1521",
    "database.user" : "c##dbzuser",
    "database.password" : "dbz_password",
    "database.dbname" : "FREE",
    "database.pdb.name" : "FREEPDB1",
    "database.server.name" : "wallet_server",
    "database.history.kafka.bootstrap.servers" : "kafka:9092",
    "database.history.kafka.topic": "schema-changes.wallet"
  }
}'
```

🎉 **เสร็จสิ้น!** 
ตอนนี้ทุกครั้งที่มีการ Insert, Update, หรือ Delete ในแอปพลิเคชัน ข้อมูลการเปลี่ยนแปลงนั้นจะถูก Debezium ดูดเข้าไปรอใน Kafka ทันทีแบบเรียลไทม์ พร้อมให้ Data Lake (PostgreSQL) ดึงไปใช้งานต่อครับ