import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image
from nav_msgs.msg import Odometry
import tf2_ros
import cv2
import numpy as np
from cv_bridge import CvBridge
from ultralytics import YOLO
import zenoh
import json
import uuid
import hashlib
from math import atan2
from builtin_interfaces.msg import Time

RUN_ID = str(uuid.uuid4())
ROBOT_ID = "tb3_sim"
sequence = 0

class DetectorNode(Node):
    def __init__(self):
        super().__init__('detector_node')
        self.bridge = CvBridge()
        self.model = YOLO('yolov8n.pt')
        self.latest_odom = None
        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)

        # Zenoh session
        conf = zenoh.Config()
        self.session = zenoh.open(conf)
        self.get_logger().info(f'Run ID: {RUN_ID}')

        # Publish run metadata
        self.session.put(
            f'maze/{ROBOT_ID}/{RUN_ID}/runmeta/v1',
            json.dumps({'run_id': RUN_ID, 'robot_id': ROBOT_ID})
        )

        self.odom_sub = self.create_subscription(Odometry, '/odom', self.odom_cb, 10)
        self.img_sub = self.create_subscription(Image, '/camera/image_raw', self.image_cb, 10)
        self.get_logger().info('Detector node started!')

    def odom_cb(self, msg):
        self.latest_odom = msg

    def image_cb(self, msg):
        global sequence
        try:
            cv_image = self.bridge.imgmsg_to_cv2(msg, 'rgb8')
        except Exception as e:
            self.get_logger().error(f'CV bridge error: {e}')
            return

        # Run YOLO
        results = self.model(cv_image, verbose=False)
        detections = []
        for r in results:
            for box in r.boxes:
                det = {
                    'det_id': str(uuid.uuid4()),
                    'class_id': int(box.cls[0]),
                    'class_name': self.model.names[int(box.cls[0])],
                    'confidence': float(box.conf[0]),
                    'bbox_xyxy': box.xyxy[0].tolist()
                }
                detections.append(det)

        # Image sha256
        sha256 = hashlib.sha256(msg.data.tobytes()).hexdigest()

        # Odometry
        odom_data = {'topic': '/odom', 'frame_id': 'odom',
                     'x': 0.0, 'y': 0.0, 'yaw': 0.0,
                     'vx': 0.0, 'vy': 0.0, 'wz': 0.0}
        if self.latest_odom:
            o = self.latest_odom
            q = o.pose.pose.orientation
            yaw = atan2(2*(q.w*q.z + q.x*q.y), 1 - 2*(q.y*q.y + q.z*q.z))
            odom_data = {
                'topic': '/odom',
                'frame_id': o.header.frame_id,
                'x': o.pose.pose.position.x,
                'y': o.pose.pose.position.y,
                'yaw': yaw,
                'vx': o.twist.twist.linear.x,
                'vy': o.twist.twist.linear.y,
                'wz': o.twist.twist.angular.z
            }

        # TF
        tf_data = {'base_frame': 'base_footprint',
                   'camera_frame': 'camera_link',
                   't_base_camera': [0.0]*16,
                   'tf_ok': False}
        try:
            t = self.tf_buffer.lookup_transform('base_footprint', 'camera_link', rclpy.time.Time())
            tr = t.transform.translation
            ro = t.transform.rotation
            tf_data['t_base_camera'] = [tr.x, tr.y, tr.z, ro.x, ro.y, ro.z, ro.w] + [0.0]*9
            tf_data['tf_ok'] = True
        except Exception:
            pass

        event_id = str(uuid.uuid4())
        sequence += 1

        event = {
            'schema': 'maze.detection.v1',
            'event_id': event_id,
            'run_id': RUN_ID,
            'robot_id': ROBOT_ID,
            'sequence': sequence,
            'image': {
                'topic': '/camera/image_raw',
                'stamp': {'sec': msg.header.stamp.sec, 'nanosec': msg.header.stamp.nanosec},
                'frame_id': msg.header.frame_id,
                'width': msg.width,
                'height': msg.height,
                'encoding': msg.encoding,
                'sha256': sha256
            },
            'odometry': odom_data,
            'tf': tf_data,
            'detections': detections
        }

        key = f'maze/{ROBOT_ID}/{RUN_ID}/detections/v1/{event_id}'
        self.session.put(key, json.dumps(event))
        self.get_logger().info(f'Published event {sequence} with {len(detections)} detections')

def main():
    rclpy.init()
    node = DetectorNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()
