"""模拟地磅：通过 MQTT QoS1 上报启动号、采样号、公斤数、稳定标记。

两种用法：
1. 交互式 REPL：
     python -m simulator.simulator --host localhost
2. 场景脚本（JSON，按数组顺序逐条发布，可人为制造缺号/补报/乱序）：
     python -m simulator.simulator --scenario scenario_stable_first.json

报文 JSON：{"startup_id": N, "seq": N, "weight_kg": x, "stable": bool}
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import paho.mqtt.client as mqtt

DEFAULT_TOPIC = "scale/weighbridge"


def build_client(host: str, port: int) -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"simulated-scale-{int(time.time())}",
        clean_session=True,
        protocol=mqtt.MQTTv311,
    )
    client.connect(host, port, keepalive=30)
    client.loop_start()
    return client


def publish(
    client: mqtt.Client,
    topic: str,
    startup_id: int,
    seq: int,
    weight_kg: float,
    stable: bool,
    *,
    retain: bool = False,
) -> None:
    payload = json.dumps(
        {
            "startup_id": startup_id,
            "seq": seq,
            "weight_kg": weight_kg,
            "stable": stable,
        },
        ensure_ascii=False,
    )
    info = client.publish(topic, payload, qos=1, retain=retain)
    info.wait_for_publish(timeout=10)
    flag = " [RETAIN]" if retain else ""
    print(
        f"-> 发布{flag} startup={startup_id} seq={seq} "
        f"weight={weight_kg}kg stable={stable}",
        flush=True,
    )


def run_scenario(client: mqtt.Client, topic: str, path: str) -> None:
    with open(path, "r", encoding="utf-8") as fh:
        steps = json.load(fh)
    if not isinstance(steps, list):
        raise SystemExit("场景文件必须是 JSON 数组")
    for step in steps:
        time.sleep(float(step.get("delay", 0.3)))
        publish(
            client,
            topic,
            int(step["startup_id"]),
            int(step["seq"]),
            float(step["weight_kg"]),
            bool(step["stable"]),
            retain=bool(step.get("retain", False)),
        )
    print("场景发送完毕", flush=True)


HELP_TEXT = """\
交互命令（回车执行）：
  startup            新启动：启动号 +1，采样号归零
  zero               发一条零读数（稳定）
  w <公斤> [stable|unstable]   发一条称重读数，如  w 3200.0 stable
  seq <n>            手动指定下一条采样号（用于制造缺号）
  retain <公斤>      发一条 retain 报文（服务端必须丢弃）
  sleep <秒>         等待
  show               查看当前启动号/下一采样号
  help               帮助
  quit               退出
"""


def run_repl(client: mqtt.Client, topic: str) -> None:
    startup_id = 1
    seq = 0
    print(HELP_TEXT)
    while True:
        try:
            line = input(f"[startup={startup_id} seq={seq}] > ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        parts = line.split()
        cmd = parts[0]
        try:
            if cmd == "startup":
                startup_id += 1
                seq = 0
                print(f"新启动：startup_id={startup_id}")
            elif cmd == "zero":
                publish(client, topic, startup_id, seq, 0.0, True)
                seq += 1
            elif cmd == "w":
                weight = float(parts[1])
                stable = len(parts) < 3 or parts[2] == "stable"
                publish(client, topic, startup_id, seq, weight, stable)
                seq += 1
            elif cmd == "seq":
                seq = int(parts[1])
            elif cmd == "retain":
                publish(
                    client, topic, startup_id, seq, float(parts[1]),
                    True, retain=True,
                )
                seq += 1
            elif cmd == "sleep":
                time.sleep(float(parts[1]))
            elif cmd == "show":
                print(f"startup_id={startup_id}, next seq={seq}")
            elif cmd in ("help", "?"):
                print(HELP_TEXT)
            elif cmd in ("quit", "exit"):
                break
            else:
                print("未知命令，输入 help 查看帮助")
        except (IndexError, ValueError) as exc:
            print(f"命令参数错误: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="模拟地磅 MQTT 上报")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--scenario", help="JSON 场景文件路径；不填则进入交互模式")
    args = parser.parse_args()

    client = build_client(args.host, args.port)
    try:
        if args.scenario:
            run_scenario(client, args.topic, args.scenario)
        else:
            run_repl(client, args.topic)
    finally:
        client.loop_stop()
        client.disconnect()
        sys.exit(0)


if __name__ == "__main__":
    main()
