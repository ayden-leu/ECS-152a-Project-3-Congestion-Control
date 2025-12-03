#!/usr/bin/env python3
"""
Minimal sender skeleton for ECS 152A project.

Purpose:
    - Send two demo packets (plus EOF marker) to verify your environment,
      receiver, and test scripts are wired up correctly.
    - Provide a tiny Stop-and-Wait style template students can extend.

Usage:
    ./test_sender.sh sender_skeleton.py [payload.zip]

Notes:
    - This is NOT a full congestion-control implementation.
    - It intentionally sends only a couple of packets so you can smoke-test
      the simulator quickly before investing time in your own sender.
    - Delay, jitter, and score calculations are hardcoded placeholders.
      Students should implement their own metrics tracking.
"""

from __future__ import annotations

import os
import socket
import sys
import time
from typing import List, Tuple

PACKET_SIZE = 1024
SEQ_ID_SIZE = 4
MSS = PACKET_SIZE - SEQ_ID_SIZE
ACK_TIMEOUT = 1.0
MAX_TIMEOUTS = 5

HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("RECEIVER_PORT", "5001"))


def verify_transfer() -> bool:
    """Verify the received file matches the original."""
    # Get original file path
    original_file = (
        os.environ.get("TEST_FILE") or os.environ.get("PAYLOAD_FILE") or "/hdd/file.zip"
    )

    basename = os.path.basename(original_file)
    if "." in basename:
        name, ext = basename.rsplit(".", 1)
        received_basename = f"{name}_received.{ext}"
    else:
        received_basename = f"{basename}_received"

    received_file = f"/hdd/{received_basename}"

    if not os.path.exists(received_file):
        print(f"✗ Received file not found: {received_file}")
        return False

    original_size = os.path.getsize(original_file)
    received_size = os.path.getsize(received_file)

    if original_size != received_size:
        print(
            f"✗ Size mismatch: original={original_size:,}, received={received_size:,}"
        )
        return False

    with open(original_file, "rb") as f1, open(received_file, "rb") as f2:
        if f1.read() == f2.read():
            print(f"✓ Transfer verified! {original_size:,} bytes sent successfully.")
            return True
        else:
            print("✗ Content mismatch")
            return False


def load_payload_chunks() -> List[bytes]:
    """
    Reads the selected payload file (or falls back to file.zip) and returns
    up to two MSS-sized chunks for the demo transfer.
    """
    candidates = [
        os.environ.get("TEST_FILE"),
        os.environ.get("PAYLOAD_FILE"),
        "/hdd/file.zip",
        "file.zip",
    ]

    for path in candidates:
        if not path:
            continue
        expanded = os.path.expanduser(path)
        if os.path.exists(expanded):
            with open(expanded, "rb") as f:
                data = f.read()
            break
    else:
        print(
            "Could not find payload file (tried TEST_FILE, PAYLOAD_FILE, file.zip)",
            file=sys.stderr,
        )
        sys.exit(1)

    if not data:
        return [b"Hello from ECS152A!", b"Second packet from skeleton sender"]

    first = data[:MSS] or b"First chunk placeholder"
    second = data[MSS : 2 * MSS] or b"Second chunk placeholder"
    return [first, second]


def make_packet(seq_id: int, payload: bytes) -> bytes:
    return int.to_bytes(seq_id, SEQ_ID_SIZE, byteorder="big", signed=True) + payload


def parse_ack(packet: bytes) -> Tuple[int, str]:
    seq = int.from_bytes(packet[:SEQ_ID_SIZE], byteorder="big", signed=True)
    msg = packet[SEQ_ID_SIZE:].decode(errors="ignore")
    return seq, msg


def print_metrics(total_bytes: int, duration: float) -> None:
    """
    Print transfer metrics in the format expected by test scripts.

    TODO: Students should replace the hardcoded delay/jitter/score values
    with actual calculated metrics from their implementation.
    """
    throughput = total_bytes / duration

    # Placeholder values - students should calculate these based on actual measurements
    avg_delay = 0.0
    avg_jitter = 0.0
    score = 0.0

    print("\nDemo transfer complete!")
    print(f"duration={duration:.3f}s throughput={throughput:.2f} bytes/sec")
    print(
        f"avg_delay={avg_delay:.6f}s avg_jitter={avg_jitter:.6f}s (TODO: Calculate actual values)"
    )
    print(f"{throughput:.7f},{avg_delay:.7f},{avg_jitter:.7f},{score:.7f}")


def main() -> None:
    demo_chunks = load_payload_chunks()
    transfers: List[Tuple[int, bytes]] = []

    seq = 0
    for chunk in demo_chunks:
        transfers.append((seq, chunk))
        seq += len(chunk)

    # EOF marker
    transfers.append((seq, b""))
    total_bytes = sum(len(chunk) for chunk in demo_chunks)

    print(f"Connecting to receiver at {HOST}:{PORT}")
    print(
        f"Demo transfer will send {total_bytes} bytes across {len(demo_chunks)} packets (+EOF)."
    )

    start = time.time()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(ACK_TIMEOUT)
        addr = (HOST, PORT)

        for seq_id, payload in transfers:
            pkt = make_packet(seq_id, payload)
            print(f"Sending seq={seq_id}, bytes={len(payload)}")

            retries = 0
            while True:
                sock.sendto(pkt, addr)

                try:
                    ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
                except socket.timeout:
                    retries += 1
                    if retries > MAX_TIMEOUTS:
                        raise RuntimeError(
                            "Receiver did not respond (max retries exceeded)"
                        )
                    print(
                        f"Timeout waiting for ACK (seq={seq_id}). Retrying ({retries}/{MAX_TIMEOUTS})..."
                    )
                    continue

                ack_id, msg = parse_ack(ack_pkt)
                print(f"Received {msg.strip()} for ack_id={ack_id}")

                if msg.startswith("fin"):
                    # Respond with FIN/ACK to let receiver exit cleanly
                    fin_ack = make_packet(ack_id, b"FIN/ACK")
                    sock.sendto(fin_ack, addr)
                    duration = max(time.time() - start, 1e-6)
                    print_metrics(total_bytes, duration)
                    return

                if msg.startswith("ack") and ack_id >= seq_id + len(payload):
                    break
                # Else: duplicate/stale ACK, continue waiting

        # Wait for final FIN after EOF packet
        while True:
            ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
            ack_id, msg = parse_ack(ack_pkt)
            if msg.startswith("fin"):
                fin_ack = make_packet(ack_id, b"FIN/ACK")
                sock.sendto(fin_ack, addr)
                duration = max(time.time() - start, 1e-6)
                print_metrics(total_bytes, duration)
                verify_transfer()
                return


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Skeleton sender hit an error: {exc}", file=sys.stderr)
        sys.exit(1)
