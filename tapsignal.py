import collections
import itertools
import json
import math
import subprocess
import sys
import threading
import time

import paho.mqtt.client as mqtt

from config import HOST, PORT, TOPIC

SAMPLE_MS = 10
BASELINE_ALPHA = 0.05
TAP_THRESHOLD = 0.8
MIN_TAP_GAP = 0.12
BURST_GAP = 0.8
GYRO_MAX = 1.0
MESSAGES = {1: "IYA", 2: "TIDAK", 3: "TOLONG"}

args = sys.argv[1:]
positional = [a for a in args if not a.startswith("--")]
device_id = positional[0] if positional else "edge1"
debug = "--debug" in args
manual = "--manual" in args
sequence = itertools.count(1)


def pick_sensor(names, key):
    found = [n for n in names if key in n.lower() and "uncalibrated" not in n.lower()]
    found.sort(key=lambda n: not n.lower().endswith("non-wakeup"))
    return found[0]


def sensor_names():
    out = subprocess.run(
        ["termux-sensor", "-l"],
        capture_output=True,
        text=True,
        check=True,
        stdin=subprocess.DEVNULL,
    ).stdout
    names = json.loads(out)["sensors"]
    return pick_sensor(names, "accelerometer"), pick_sensor(names, "gyroscope")


def read_stream(accel, gyro):
    proc = subprocess.Popen(
        ["termux-sensor", "-s", f"{accel},{gyro}", "-d", str(SAMPLE_MS)],
        stdout=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
    )
    decoder = json.JSONDecoder()
    buffer = ""
    for line in proc.stdout:
        buffer += line
        try:
            obj, _ = decoder.raw_decode(buffer.lstrip())
        except json.JSONDecodeError:
            continue
        buffer = ""
        yield obj


class TapDetector:
    def __init__(self):
        self.baseline = None
        self.window = collections.deque(maxlen=3)
        self.last_tap = 0.0
        self.gyro_w = 0.0

    def feed_gyro(self, wx, wy, wz):
        self.gyro_w = math.sqrt(wx * wx + wy * wy + wz * wz)

    def feed_accel(self, x, y, z, now):
        mag = math.sqrt(x * x + y * y + z * z)
        if self.baseline is None:
            self.baseline = mag
        dev = abs(mag - self.baseline)
        self.baseline += BASELINE_ALPHA * (mag - self.baseline)
        self.window.append((dev, now))
        if len(self.window) < 3:
            return None
        (d0, _), (d1, t1), (d2, _) = self.window
        is_peak = d1 >= TAP_THRESHOLD and d1 >= d0 and d1 > d2
        if not is_peak or t1 - self.last_tap < MIN_TAP_GAP:
            return None
        if self.gyro_w > GYRO_MAX:
            return None
        self.last_tap = t1
        return d1


def buzz(code, message, sender):
    subprocess.run(
        [
            "termux-notification",
            "--id",
            "tapsignal",
            "--title",
            "TapSignal",
            "--content",
            f"{sender}: {message}",
            "--priority",
            "high",
        ],
        stdin=subprocess.DEVNULL,
    )
    for _ in range(code):
        subprocess.run(["termux-vibrate", "-d", "300", "-f"], stdin=subprocess.DEVNULL)
        time.sleep(0.6)


def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe(f"{TOPIC}/messages", qos=1)
    print(f"[{device_id}] terhubung ke broker")


def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    if data["device_id"] == device_id:
        return
    print(f"[TERIMA] {data['device_id']}: {data['message']} ({data['code']} ketukan)")
    threading.Thread(
        target=buzz, args=(data["code"], data["message"], data["device_id"]), daemon=True
    ).start()


def send(client, count):
    message = MESSAGES.get(count)
    if message is None:
        print(f"[ABAIKAN] {count} ketukan")
        return
    payload = {
        "device_id": device_id,
        "sequence": next(sequence),
        "code": count,
        "message": message,
        "sent_at_ms": int(time.time() * 1000),
    }
    client.publish(f"{TOPIC}/messages", json.dumps(payload), qos=1)
    print(f"[KIRIM] {message} ({count} ketukan)")


def manual_loop(client):
    print("Mode manual: ketik 1, 2, atau 3 lalu Enter")
    for line in sys.stdin:
        if line.strip().isdigit():
            send(client, int(line.strip()))


def sensor_loop(client):
    accel, gyro = sensor_names()
    detector = TapDetector()
    taps = []
    print(f"[{device_id}] siap, ketuk meja di dekat HP")
    for obj in read_stream(accel, gyro):
        now = time.monotonic()
        for name, payload in obj.items():
            v = payload["values"]
            if name == accel:
                peak = detector.feed_accel(v[0], v[1], v[2], now)
                if peak is not None:
                    taps.append(now)
                    if debug:
                        print(f"[PEAK] dev={peak:.2f} gyro={detector.gyro_w:.2f}")
            elif name == gyro:
                detector.feed_gyro(v[0], v[1], v[2])
        if taps and now - taps[-1] > BURST_GAP:
            count = len(taps)
            taps.clear()
            send(client, count)


def main():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"tapsignal-{device_id}-{int(time.time())}",
    )
    client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(HOST, PORT, keepalive=30)
    client.loop_start()
    if manual:
        manual_loop(client)
    else:
        sensor_loop(client)


if __name__ == "__main__":
    main()