import zenoh
import json
import psycopg2
import uuid
from datetime import datetime, timezone

DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'robotics',
    'user': 'turtlebot',
    'password': 'turtlebot'
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def ingest(sample):
    try:
        event = json.loads(sample.payload.to_bytes())
        conn = get_conn()
        cur = conn.cursor()

        stamp = datetime.fromtimestamp(
            event['image']['stamp']['sec'] + event['image']['stamp']['nanosec'] * 1e-9,
            tz=timezone.utc
        )

        cur.execute('''
            INSERT INTO detection_events
            (event_id, run_id, robot_id, sequence, stamp,
             image_frame_id, image_sha256, width, height, encoding,
             x, y, yaw, vx, vy, wz,
             tf_ok, t_base_camera, raw_event)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        ''', (
            event['event_id'], event['run_id'], event['robot_id'], event['sequence'], stamp,
            event['image']['frame_id'], event['image']['sha256'],
            event['image']['width'], event['image']['height'], event['image']['encoding'],
            event['odometry']['x'], event['odometry']['y'], event['odometry']['yaw'],
            event['odometry']['vx'], event['odometry']['vy'], event['odometry']['wz'],
            event['tf']['tf_ok'], event['tf']['t_base_camera'],
            json.dumps(event)
        ))

        for det in event.get('detections', []):
            cur.execute('''
                INSERT INTO detections
                (event_id, det_id, class_id, class_name, confidence, x1, y1, x2, y2)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            ''', (
                event['event_id'], det['det_id'], det['class_id'], det['class_name'],
                det['confidence'],
                det['bbox_xyxy'][0], det['bbox_xyxy'][1],
                det['bbox_xyxy'][2], det['bbox_xyxy'][3]
            ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"Ingested event {event['sequence']} with {len(event.get('detections',[]))} detections")

    except Exception as e:
        print(f'Error: {e}')

def main():
    conf = zenoh.Config()
    session = zenoh.open(conf)
    print('Ingest worker started, waiting for events...')
    sub = session.declare_subscriber('maze/**/detections/v1/*', ingest)
    try:
        while True:
            pass
    except KeyboardInterrupt:
        sub.undeclare()
        session.close()

if __name__ == '__main__':
    main()
