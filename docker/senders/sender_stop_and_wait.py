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
MAX_TIMEOUTS:int = 3

HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("RECEIVER_PORT", "5001"))


## Stop-and-wait:  Send a packet, wait for an ACK, send another packet, repeat

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

	print(f"Connecting to receiver at {HOST}:{PORT}")
	print(
		f"Sending {totalBytes} bytes across {len(payloadChunks)} packets (+EOF)."
	)

	RTTs:List[float] = []
	totalRetransmissions:int = 0

	timeStart = time.time()
	with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
		sock.settimeout(ACK_TIMEOUT)
		address = (HOST, PORT)

		for sequenceID, payload in chunksToSend:
			packet = makePacket(sequenceID, payload)
			print(f"Sending seq={sequenceID}, bytes={len(payload)}")

			retries = 0
			while True:
				timePacketSent = time.time()
				try:
					sock.sendto(packet, address)
				except Exception as e:
					raise RuntimeError(f"failed to send initial packet (seq_id: {sequenceID})")

				try:
					ACKpacket, _ = sock.recvfrom(PACKET_SIZE)
				except socket.timeout:
					retries += 1
					if retries > MAX_TIMEOUTS:
						raise RuntimeError(
							"Receiver did not respond (max retries exceeded)"
						)
					print(
						f"Timeout waiting for ACK (seq={sequenceID}). Retrying ({retries}/{MAX_TIMEOUTS})..."
					)
					continue
				except Exception as e:
					raise RuntimeError(f"got an error: {e}")

				ACKid, message = parseACK(ACKpacket)
				print(f"Received {message.strip()} for ack_id={ACKid}")

				# end condition
				if message.startswith("fin"):
					# Respond with FIN/ACK to let receiver exit cleanly
					finalACK = makePacket(ACKid, b"FIN/ACK")
					sock.sendto(finalACK, address)
					duration = max(time.time() - timeStart, 1e-6)
					printMetrics(
						totalBytes=totalBytes,
						duration=duration,
						RTTs=RTTs
					)
					return

				# ack received
				if message.startswith("ack") and ACKid >= sequenceID + len(payload):
					timeRecievedACK = time.time()
					timeToReceiveACK = timeRecievedACK - timePacketSent

					if timeToReceiveACK >= 0:
						RTTs.append(timeToReceiveACK)
						print(f"ack_RTT: {timeToReceiveACK}")
					
					retries = 0
					break

				# Else: duplicate/stale ACK, continue waiting
				else:
					print("Duplicate/stale ACK, will continue waiting...")

		# Wait for final FIN after EOF packet
		while True:
			ACKpacket, _ = sock.recvfrom(PACKET_SIZE)
			ACKid, message = parseACK(ACKpacket)
			if message.startswith("fin"):
				finalACK = makePacket(ACKid, b"FIN/ACK")
				sock.sendto(finalACK, address)
				duration = max(time.time() - timeStart, 1e-6)
				printMetrics(totalBytes, duration, RTTs=RTTs)
				return


if __name__ == "__main__":
	try:
		main()
	except Exception as exc:
		print(f"Skeleton sender hit an error: {exc}", file=sys.stderr)
		sys.exit(1)
