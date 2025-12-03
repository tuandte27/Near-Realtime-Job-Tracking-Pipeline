import time
import json
import os
import datetime
from datetime import datetime, timedelta
from cassandra.cluster import Cluster, NoHostAvailable
from uuid import UUID
from kafka import KafkaProducer

CASSANDRA_HOST = 'cassandra'
KAFKA_HOST = 'broker:29092'
TOPIC = 'cassandra_cdc.cassandra_data.tracking'
POLL_INTERVAL = 0.5
OFFSET_FILE = '/app/last_scan.json'

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def cassandra_to_dict(row):
    return {
        "create_time": str(row.create_time),
        "bid": float(row.bid) if row.bid is not None else None,
        "bn": row.bn,
        "campaign_id": int(row.campaign_id) if row.campaign_id is not None else None,
        "cd": int(row.cd) if row.cd is not None else None,
        "custom_track": row.custom_track,
        "de": row.de,
        "dl": row.dl,
        "dt": row.dt,
        "ed": row.ed,
        "ev": int(row.ev) if row.ev is not None else None,
        "group_id": int(row.group_id) if row.group_id is not None else None,
        "id": row.id,
        "job_id": int(row.job_id) if row.job_id is not None else None,
        "md": row.md,
        "publisher_id": int(row.publisher_id) if row.publisher_id is not None else None,
        "rl": row.rl,
        "sr": row.sr,
        "ts": int(row.ts.timestamp() * 1000) if row.ts else None,
        "tz": int(row.tz) if row.tz is not None else None,
        "ua": row.ua,
        "uid": row.uid,
        "utm_campaign": row.utm_campaign,
        "utm_content": row.utm_content,
        "utm_medium": row.utm_medium,
        "utm_source": row.utm_source,
        "utm_term": row.utm_term,
        "v": int(row.v) if row.v is not None else None,
        "vp": row.vp
    }


def get_last_scan_time():
    try:
        with open(OFFSET_FILE, 'r') as f:
            return datetime.fromisoformat(json.load(f)['last_scan_time'])
    except:
        return datetime(1970,1,1)

def save_last_scan_time(time_obj):
    with open(OFFSET_FILE, 'w') as f:
        json.dump({'last_scan_time': time_obj.isoformat()}, f)

def connect_to_cluster():
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=9042)
            session = cluster.connect('cassandra_data')
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_HOST],
                value_serializer=json_serializer
            )
            print("✅ Connected to Cassandra & Kafka")
            return cluster, session, producer
        except Exception as e:
            print(f"⚠️ Connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

def main():
    cluster, session, producer = connect_to_cluster()
    last_scan = get_last_scan_time()
    print(f"🚀 CDC Producer Started. Scanning all data since: {last_scan}")

    try:
        while True:
            loop_start_time = datetime.now()
            
            cql = f"SELECT * FROM tracking WHERE ts > %s ALLOW FILTERING"
            
            try:
                rows = session.execute(cql, (last_scan,))
                sent = 0
                
                current_batch_max_ts = last_scan

                for row in rows:
                    if row.ts and row.ts > last_scan:
                        payload_after = cassandra_to_dict(row)
                        message = {
                            "payload": {
                                "after": payload_after,
                                "op": "r" if last_scan.year == 1970 else "c",
                                "ts_ms": int(time.time() * 1000),
                            }
                        }
                        producer.send(TOPIC, message)
                        sent += 1
                        
                        if row.ts > current_batch_max_ts:
                            current_batch_max_ts = row.ts
                
                if sent > 0:
                    print(f"✅ [Sync] Sent {sent} events. Updated last_scan to: {current_batch_max_ts}")
                    # Có dữ liệu mới -> Cập nhật mốc thời gian theo dữ liệu đó
                    last_scan = current_batch_max_ts
                else:
                    last_scan = loop_start_time
                
                save_last_scan_time(last_scan)

            except Exception as e:
                print(f"❌ Error: {e}. Retrying...")

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("🛑 Stopping Producer...")
    finally:
        cluster.shutdown()
        producer.close()

if __name__ == "__main__":
    main()