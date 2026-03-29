# Run Report — Assignment 2

## Pipeline Summary
- Total events ingested: 73
- Total detections: 0
- Robot ID: tb3_sim
- ROS Distribution: jazzy

## Class Histogram
- No objects detected (geometric placeholder objects used)

## System Components
- Detector: YOLOv8n (ultralytics)
- Transport: Eclipse Zenoh
- Database: PostgreSQL 15
- Key pattern: maze/<robot_id>/<run_id>/detections/v1/<event_id>

## Reproducibility
1. Start demo-world: docker compose up demo-world
2. Start ingest worker: python3 ingest_worker.py
3. Start detector: python3 detector_node.py
