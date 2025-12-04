#!/usr/bin/env python3
"""
Implementation for Fixed Sliding Window
Started from sender_skeleton.py, modified to send the entire file with a 100 packet sized window
"""

# from __future__ import annotations 
# Above is from sender_skeleton.py but is not used/needed

import os
import socket
import sys
import time
from typing import List, Tuple

PACKET_SIZE = 1024
SEQ_ID_SIZE = 4
MSS = PACKET_SIZE - SEQ_ID_SIZE

# Changed the timeout for waiting for ACKs from 1.0 to 2.0 seconds (bc of bigger window size)
ACK_TIMEOUT = 2.0 

# MAX_TIMEOUTS = 5 
# Above is from sender_skeleton.py but is not used/needed

# Fixed window size of 100 packets, as specified in project instructions
WINDOW_SIZE = 100 

HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("RECEIVER_PORT", "5001"))


# Function below was modified from sender_skeleton.py; 
# Initial function only read up to 2 chunks, modified to read the entire file
# Renamed from load_payload_chunks to load_data since we just load the data here
# Chunks will be created later in main()
def load_data() -> bytes:
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
                return f.read()
            
    print(
        "Could not find payload file (tried TEST_FILE, PAYLOAD_FILE, file.zip)",
        file=sys.stderr,
    )
    sys.exit(1)

# Function below is unchanged from sender_skeleton.py
def make_packet(seq_id: int, payload: bytes) -> bytes:
    return int.to_bytes(seq_id, SEQ_ID_SIZE, byteorder="big", signed=True) + payload

# Function below is unchanged from sender_skeleton.py
def parse_ack(packet: bytes) -> Tuple[int, str]:
    seq = int.from_bytes(packet[:SEQ_ID_SIZE], byteorder="big", signed=True)
    msg = packet[SEQ_ID_SIZE:].decode(errors="ignore")
    return seq, msg

# Function below is modified from sender_skeleton.py to actually include the metrics printing
def print_metrics(total_bytes: int, start_time: float, delays: list[float]) -> None:
    """
    Computes and prints the throughput, average (per-packet) delay, average jitter, and overall metric
    """
    
    duration = time.time() - start_time

    # Throughput
    throughput = 0.0
    if duration > 0:
        throughput = total_bytes / duration
    
    # Average Delay
    average_delay = 0.0
    if delays:
        average_delay = sum(delays) / len(delays)
    
    # Jitter
    average_jitter = 0.0
    if len(delays) > 1:
        jitter_values = []
        for i in range(1, len(delays)):
            jitter_values.append(abs(delays[i] - delays[i - 1]))
        average_jitter = sum(jitter_values) / len(jitter_values)
    
    # Metric based on the formula provided (Performance Metric = (Throughput / 2000) + (15 / Average Jitter) + (35 / Average Delay))
    metric = 0.0
    metric += (throughput / 2000)
    if average_jitter > 0:
        metric += (15 / average_jitter)
    if average_delay > 0:
        metric += (35 / average_delay)

    print("\nTransfer complete!")
    print(f"duration={duration:.3f}s throughput={throughput:.2f} bytes/sec")
    print(
        f"avg_delay={average_delay:.6f}s avg_jitter={average_jitter:.6f}s"
    )
    # Line below prints in format as defined in project instructions (four comma separated values)
    print(f"{throughput:.7f},{average_delay:.7f},{average_jitter:.7f},{metric:.7f}")


def main():
    # Read the whole file at once instead of whatwas in the skeleton which only read 2 chunks
    data = load_data()

    transfers: List[Tuple[int, bytes]] = [] 
    seq = 0
    # Splits data into chunks of size MSS and appends to the transfers list
    for i in range(0, len(data), MSS):
        chunk = data[i : i + MSS]
        transfers.append((seq, chunk))
        seq += len(chunk)

    # added a EOF marker with empty payload at the very end
    transfers.append((seq, b""))
    total_bytes = len(data)

    # UDP socket and timeout for ACKs
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(ACK_TIMEOUT)
    addr = (HOST, PORT)

    # smal pause helps us make sure receiver is up before packets start being sent
    time.sleep(1.0) 
    start = time.time()

    # For metrics
    send_times = {} # Dictionary traks send times for each packet
    delays = [] # List tracks delays for each packet

    # indexes for sliding window
    base = 0 # base = index of 1st unacked packet
    next_to_send = 0 # next packet index to be sent

    try:
        while base < len(transfers):
            # Send packets in the window
            window_limit = base + WINDOW_SIZE
            while next_to_send < len(transfers) and next_to_send < window_limit:
                seq_id, payload = transfers[next_to_send]
                pkt = make_packet(seq_id, payload)

                if seq_id not in send_times:
                    send_times[seq_id] = time.time() # Records the send time

                sock.sendto(pkt, addr)
                # print(f"Sending seq={seq_id}, bytes={len(payload)}")
                next_to_send += 1
            
            # Receive/Wait for ACKs
            try: 
                ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
                ack_id, msg = parse_ack(ack_pkt)

                # sort of taken from sender_skeleton but modified since we aren't just doing a send and wait and this is for more than 2 packets
                if msg.startswith("fin"):
                    fin_ack = make_packet(ack_id, b"FIN/ACK")
                    sock.sendto(fin_ack, addr)
                    break
                # Base is moved forward for every packet that ack_id accounts for
                while base < len(transfers):
                    seq_id, payload = transfers[base]
                    next_expected = seq_id + len(payload)

                    if ack_id >= next_expected:
                        # Calculates the packet delay
                        if seq_id in send_times:
                            delay = time.time() - send_times[seq_id]
                            delays.append(delay)
                        base += 1
                    else:
                        # current ACK hasn't reached this packet yet
                        break
            except socket.timeout:
                # in case of timeout, resend packets starting from the first unacked packet
                next_to_send = base
    finally:
        sock.close()

    print_metrics(total_bytes, start, delays)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Skeleton sender hit an error: {exc}", file=sys.stderr)
        sys.exit(1)
