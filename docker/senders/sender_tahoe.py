#!/usr/bin/env python3
from __future__ import annotations

import os
import socket
import sys
import time
from typing import List, Tuple

PACKET_SIZE:int = 1024
SEQ_ID_SIZE:int = 4
MAX_SEGMENT_SIZE:int = PACKET_SIZE - SEQ_ID_SIZE  # accounts for the sequence ID every packet needs
ACK_TIMEOUT:float = 5.0

DELAY_UNTIL_START:float = 0.5
INITIAL_CONGESTION_WINDOW:int = 1
INITIAL_SLOW_START_THRESHOLD:int = 64
FAST_RETRANSMIT_THRESHOLD:int = 3

HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("RECEIVER_PORT", "5001"))


def splitPayloadIntoChunks() -> List[bytes]:
	candidates = [
		os.environ.get("TEST_FILE"),
		os.environ.get("PAYLOAD_FILE"),
		# "/hdd/file.zip",  # big file
		# "file.zip",       # big file
	]

	# determine whata the payload file actually is
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
		return [b"error:", b"couldn't load any payload candidates"]

	payloadInChunks:List[bytes] = []
	offset:int = 0
	while offset < len(data):
		chunk = data[offset : offset + MAX_SEGMENT_SIZE]
		payloadInChunks.append(chunk)
		offset += MAX_SEGMENT_SIZE

	return payloadInChunks


def makePacket(sequenceID: int, payload: bytes) -> bytes:
	return int.to_bytes(sequenceID, SEQ_ID_SIZE, byteorder="big", signed=True) + payload

def parseACK(packet: bytes) -> Tuple[int, str]:
	sequence = int.from_bytes(packet[:SEQ_ID_SIZE], byteorder="big", signed=True)
	message = packet[SEQ_ID_SIZE:].decode(errors="ignore")
	return sequence, message

# def sumList(sourceList:List[float]) -> float:
# 	total:float = 0.0
# 	for entry in sourceList:
# 		total += entry
# 	return total

def printMetrics(totalBytes:int, duration:float, RTTs:List[float]=None) -> None:
	"""
	Print transfer metrics in the format expected by test scripts.
	"""
	
	avgDelay:float = 0.0
	avgJitter:float = 0.0
	if RTTs:
		avgDelay = sum(RTTs) / len(RTTs)

		changesInRTT:List[float] = []
		for i in range(1, len(RTTs)):
			changesInRTT.append(abs(RTTs[i] - RTTs[i-1]))
		avgJitter = sum(changesInRTT) / len(changesInRTT)
	
	throughput = totalBytes / duration if duration > 0 else 0.0
	score:float = (throughput/2000)
	if avgJitter > 0:
		score += (15/avgJitter)
	if avgDelay > 0:
		score += (35/avgDelay)

	print("\nMetrics:")
	print(f"duration={duration:.3f}s throughput={throughput:.2f} bytes/sec")
	print(
		f"avg_delay={avgDelay:.6f}s avg_jitter={avgJitter:.6f}s"
	)
	print(f"{throughput:.7f},{avgDelay:.7f},{avgJitter:.7f},{score:.7f}")


def main() -> None:
    payloadChunks = splitPayloadIntoChunks()
    chunksToSend: List[Tuple[int, bytes]] = []

    sequence = 0
    for chunk in payloadChunks:
        chunksToSend.append((sequence, chunk))
        sequence += len(chunk)

    # EOF marker
    chunksToSend.append((sequence, b""))
    totalBytes = sum(len(chunk) for chunk in payloadChunks)

    # print(f"Connecting to receiver at {HOST}:{PORT}")
    # print(
    #   f"Sending {totalBytes} bytes across {len(payloadChunks)} packets (+EOF)."
    # )

    RTTs: List[float] = []
    # totalRetransmissions:int = 0

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(ACK_TIMEOUT)
        address = (HOST, PORT)

        # extra precaution just in case receiver isnt up in time
        time.sleep(DELAY_UNTIL_START)
        timeStart = time.time()

        # TCP Tahoe attributes
        oldestUnACKedPacketNum: int = 0
        nextUnACKedPacketToSend: int = 0
        congestionWindow: float = float(INITIAL_CONGESTION_WINDOW)
        slowStartThreshold: int = INITIAL_SLOW_START_THRESHOLD
        numDuplicateACKs: int = 0
        lastACKid: int = -1

        # we can send multiple packets now, so we gotta store them somewhere
        packetSendTimestamps: dict[int, float] = {}

        while oldestUnACKedPacketNum < len(chunksToSend):
            # send packets that are in the congestion window
            windowEnd = oldestUnACKedPacketNum + int(congestionWindow)
            while (
                nextUnACKedPacketToSend < len(chunksToSend)
                and nextUnACKedPacketToSend < windowEnd
            ):
                sequenceID, payload = chunksToSend[nextUnACKedPacketToSend]
                packet = makePacket(sequenceID, payload)
                packetSendTimestamps[sequenceID] = time.time()

                # print(f"Sending seq={sequenceID}, bytes={len(payload)}")
                try:
                    sock.sendto(packet, address)
                except Exception as e:
                    raise RuntimeError(
                        f"failed to send initial packet (seq_id: {sequenceID})"
                    )
                nextUnACKedPacketToSend += 1
            
            retries = 0
            try:
                ACKpacket, _ = sock.recvfrom(PACKET_SIZE)

                ACKid, message = parseACK(ACKpacket)
                # print(f"Received {message.strip()} for ack_id={ACKid}")

                # end condition
                if message.startswith("fin"):
                    # Respond with FIN/ACK to let receiver exit cleanly
                    finalACK = makePacket(ACKid, b"FIN/ACK")
                    sock.sendto(finalACK, address)
                    duration = max(time.time() - timeStart, 1e-6)
                    printMetrics(
                        totalBytes=totalBytes,
                        duration=duration,
                        RTTs=RTTs,
                    )
                    return

                # handle duplicate ACKs
                if ACKid == lastACKid:
                    numDuplicateACKs += 1

                    # do fast retransmit
                    if numDuplicateACKs == FAST_RETRANSMIT_THRESHOLD:
                        # retransmit packets starting from the current base
                        if oldestUnACKedPacketNum < len(chunksToSend):
                            sequenceID, payload = chunksToSend[oldestUnACKedPacketNum]
                            packet = makePacket(sequenceID, payload)
                            sock.sendto(packet, address)

                        # avoid congestion
                        slowStartThreshold = max(int(congestionWindow / 2), 1)
                        congestionWindow = float(slowStartThreshold)
                        numDuplicateACKs = 0
                # ack received
                else:
                    numDuplicateACKs = 0
                    lastACKid = ACKid

                    # slide window forward
                    oldBase = oldestUnACKedPacketNum
                    while oldestUnACKedPacketNum < len(chunksToSend):
                        sequenceID, payload = chunksToSend[oldestUnACKedPacketNum]
                        nextExpectedACKid = sequenceID + len(payload)

                        if ACKid >= nextExpectedACKid:
                            # packets successfully sent
                            if sequenceID in packetSendTimestamps:
                                packetRTT = time.time() - packetSendTimestamps[
                                    sequenceID
                                ]
                                RTTs.append(packetRTT)
                                # print(f"packet RTT: {packetRTT}")
                            oldestUnACKedPacketNum += 1
                        else:
                            break

                    # update congestion window
                    numPacketsACKed = oldestUnACKedPacketNum - oldBase
                    if numPacketsACKed > 0:
                        # slow start
                        if congestionWindow < slowStartThreshold:
                            congestionWindow += numPacketsACKed
                        # congestion avoidance
                        else:
                            congestionWindow += numPacketsACKed / congestionWindow

            except socket.timeout:
                # slow start due to timeout
                slowStartThreshold = max(int(congestionWindow / 2), 1)
                congestionWindow = float(INITIAL_CONGESTION_WINDOW)
                numDuplicateACKs = 0

                # retransmit packets starting from the current base
                nextUnACKedPacketToSend = oldestUnACKedPacketNum

        # Wait for final FIN after EOF packet
        while True:
            ACKpacket, _ = sock.recvfrom(PACKET_SIZE)
            ACKid, message = parseACK(ACKpacket)
            if message.startswith("fin"):
                finalACK = makePacket(ACKid, b"FIN/ACK")
                sock.sendto(finalACK, address)
                duration = time.time() - timeStart
                printMetrics(totalBytes, duration, RTTs)
                return
if __name__ == "__main__":
	try:
		main()
	except Exception as exc:
		print(f"TCP Tahoe sender error: {exc}", file=sys.stderr)
		sys.exit(1)
