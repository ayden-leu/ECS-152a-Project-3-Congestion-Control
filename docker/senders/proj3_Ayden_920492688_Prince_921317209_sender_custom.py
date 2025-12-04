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

# Vegas
VEGAS_MIN_THROUGHPUT_DIFFERENCE:int = 2  # (ALPHA) if throughput difference < alpha, increase window
VEGAS_MAX_THROUGHPUT_DIFFERENCE:int = 4  # (BETA)  if its > beta, decrease
VEGAS_AMOUNT_TO_CHANGE:int = 1  # (GAMMA) amount to change window size by
RECENT_RTT_SAMPLES_TO_KEEP:int = 10
VEGAS_CONGESTION_AVOIDANCE_THRESHOLD:int = 3

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

	with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
		sock.settimeout(ACK_TIMEOUT)
		address = (HOST, PORT)

		# extra precaution just in case receiver isnt up in time
		time.sleep(DELAY_UNTIL_START)
		timeStart = time.time()

		# TCP Tahoe attributes
		oldestUnACKedPacketNum:int = 0
		nextUnACKedPacketToSend:int = 0
		congestionWindow:int = INITIAL_CONGESTION_WINDOW
		slowStartThreshold:int = INITIAL_SLOW_START_THRESHOLD
		numDuplicateACKs:int = 0
		lastACKid:int = -1

		# inFastRecovery:bool = False

		# we can send multiple packets now, so we gotta store them somewhere
		packetSendTimestamps = {}

		# Vegas attributes
		minimumRTT = None
		vegasRTTs = []
		weShouldDoVegas:bool = False

		while oldestUnACKedPacketNum < len(chunksToSend):
			# print(f"1: oldestUnACKedPacketNum: {oldestUnACKedPacketNum}")
			# print(f"1: chunksToSend length: {len(chunksToSend)}")
			windowEnd = oldestUnACKedPacketNum + congestionWindow
			# send packets that are in the congestion window
			while nextUnACKedPacketToSend < len(chunksToSend) \
				and nextUnACKedPacketToSend < oldestUnACKedPacketNum + congestionWindow:

				# print(f"2: nextUnACKedPacketToSend: {nextUnACKedPacketToSend}")
				# print(f"2: chunksToSend length: {len(chunksToSend)}")
				# print(f"2: oldestUnACKedPacketNum: {oldestUnACKedPacketNum}")
				# print(f"2: congestionWindow: {congestionWindow}")

				sequenceID, payload = chunksToSend[nextUnACKedPacketToSend]
				packet = makePacket(sequenceID, payload)
				packetSendTimestamps[sequenceID] = time.time()

				print(f"Sending seq={sequenceID}, bytes={len(payload)}")
				try:
					sock.sendto(packet, address)
				except Exception as e:
					raise RuntimeError(f"failed to send initial packet (seq_id: {sequenceID})")
				nextUnACKedPacketToSend += 1
			
			print("Waiting for packet....")
			try:
					ACKpacket, _ = sock.recvfrom(PACKET_SIZE)

					ACKid, message = parseACK(ACKpacket)
					print(f"Received {message.strip()} for ack_id={ACKid}")

					# end condition
					if message.startswith("fin"):
						# print("finale")
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

					# handle duplicate ACKs
					if ACKid == lastACKid:
						# print("duplicated time")
						numDuplicateACKs += 1

						# if inFastRecovery:
						# 	# increase congestion window size for each duplicate ACK
						# 	congestionWindow += 1

						# do fast retransmit
						# elif
						if numDuplicateACKs == FAST_RETRANSMIT_THRESHOLD:
							# print("fast retransmit threshold")
							# retransmit packets starting from the current base
							if oldestUnACKedPacketNum < len(chunksToSend):
								sequenceID, payload = chunksToSend[oldestUnACKedPacketNum]
								packet = makePacket(sequenceID, payload)
								sock.sendto(packet, address)
							
							# avoid congestion
							slowStartThreshold = max(int(congestionWindow / 2), 2)
							# congestionWindow = slowStartThreshold + FAST_RETRANSMIT_THRESHOLD 
							# inFastRecovery = True
							congestionWindow = INITIAL_CONGESTION_WINDOW
							weShouldDoVegas = False
							numDuplicateACKs = 0

						# print(f"3: nextUnACKedPacketToSend: {nextUnACKedPacketToSend}")
						# print(f"3: chunksToSend length: {len(chunksToSend)}")
						# print(f"3: oldestUnACKedPacketNum: {oldestUnACKedPacketNum}")
						# print(f"3: congestionWindow: {congestionWindow}")

					# ack received
					else:
						# if inFastRecovery:
						# 	# exit fast recovery as duplicate ACKs are no more for now
						# 	congestionWindow = slowStartThreshold
						# 	inFastRecovery = False

						numDuplicateACKs = 0
						lastACKid = ACKid

						# slide window forward
						oldBase = oldestUnACKedPacketNum
						currentRTT = None

						while oldestUnACKedPacketNum < len(chunksToSend):
							sequenceID, payload = chunksToSend[oldestUnACKedPacketNum]
							nextExpectedACKid = sequenceID + len(payload)

							if ACKid >= nextExpectedACKid:
								# packets successfully sent
								if sequenceID in packetSendTimestamps:
									packetRTT = time.time() - packetSendTimestamps[sequenceID]
									RTTs.append(packetRTT)
									print(f"packet RTT: {packetRTT}")
									currentRTT = packetRTT

									# keep track of minimum RTT for Vegas
									if minimumRTT == None or packetRTT < minimumRTT:
										minimumRTT = packetRTT
									
									# keep X most recent RTT samples
									vegasRTTs.append(packetRTT)
									if len(vegasRTTs) > RECENT_RTT_SAMPLES_TO_KEEP:
										vegasRTTs.pop(0)

								oldestUnACKedPacketNum += 1
							else:
								break
						

						# update congestion window IF not in fast recovery
						# if not inFastRecovery:
						numPacketsACKed = oldestUnACKedPacketNum - oldBase
						if numPacketsACKed > 0 and currentRTT != None:
							# slow start
							if congestionWindow < slowStartThreshold:
								congestionWindow += numPacketsACKed
								# switch to Vegas when threshold met
								if congestionWindow >= slowStartThreshold:
									weShouldDoVegas = True
							
							elif weShouldDoVegas and minimumRTT != None and len(vegasRTTs) >= VEGAS_CONGESTION_AVOIDANCE_THRESHOLD:
								# time to calculate expected and actual throughput
								avgRTT = sum(vegasRTTs) / len(vegasRTTs)
								expectedThroughput = congestionWindow / minimumRTT
								actualThroughput = congestionWindow / avgRTT
								throughputDifference = (expectedThroughput - actualThroughput) * minimumRTT

								# iconic Vegas decision making
								if throughputDifference < VEGAS_MIN_THROUGHPUT_DIFFERENCE:
									congestionWindow += VEGAS_AMOUNT_TO_CHANGE
								elif throughputDifference > VEGAS_MAX_THROUGHPUT_DIFFERENCE:
									congestionWindow = max(congestionWindow - VEGAS_AMOUNT_TO_CHANGE, 2)
								# else, difference is between ALPHA and BETA, so no change

							# congestion avoidance if we should not do Vegas
							elif not weShouldDoVegas:
								congestionWindow += int(numPacketsACKed / congestionWindow)
				
			except socket.timeout:
				# slow start due to timeout
				# inFastRecovery = False
				slowStartThreshold = max(int(congestionWindow / 2), 1)
				congestionWindow = INITIAL_CONGESTION_WINDOW
				numDuplicateACKs = 0
				
				# retransmit packets starting from the current base
				nextUnACKedPacketToSend = oldestUnACKedPacketNum

				weShouldDoVegas = False

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
		print(f"Custom sender error: {exc}", file=sys.stderr)
		sys.exit(1)
