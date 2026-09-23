## The code provided here is just a skeleton that can help you get started
## You can add/remove functions as you wish

## import (add more if you need)
from socket import *
import sys
import unreliable_channel
import zlib
import time
import threading
import struct

#In unreliable_channel we had to change received_data+= "corrupted!" to received_data+= b"corrupted!"
#We needed to make it into byte type instead of it being text

DATA = 0
ACK = 1
HEADER_FORMAT = "!IIII"
HEADER_SIZE = 16

#We kept the skeleton provided method and added the logic for it to run
def create_ack_packet(seq_num):
	# create an ACK packet acknowledging receipt of seq_num
	payload = b""
	packetType = ACK
	packetLength = HEADER_SIZE
	tempHeader = struct.pack(HEADER_FORMAT, packetType, seq_num, packetLength, 0)
	checksum = zlib.crc32(tempHeader + payload) & 0xffffffff

	finalHeader = struct.pack(HEADER_FORMAT, packetType, seq_num, packetLength, checksum)
	return finalHeader + payload

# Two types of packets, data and ack
# ack for receiver and data for sender
# crc32 available through zlib library

#We kept the skeleton provided method and added the logic for it to run
#We added packet parsing logic to unpack the header and validate packet length, we also recalculate checksum and note if we have corruption
def extract_packet_info(packetBytes):
	if len(packetBytes) < HEADER_SIZE:
		return None, None, None, None, None, b"", True
	headerBytes = packetBytes[:HEADER_SIZE]
	packetType, sequenceNumber, packetLength, checksumInPacket = struct.unpack(HEADER_FORMAT, headerBytes)

	if packetLength < HEADER_SIZE or packetLength != len(packetBytes):
		return packetType, sequenceNumber, packetLength, checksumInPacket, None, b"", True
	
	payload = packetBytes[HEADER_SIZE:packetLength]
	recomputedHeader = struct.pack(HEADER_FORMAT, packetType, sequenceNumber, packetLength, 0)
	checksumCalculated = zlib.crc32(recomputedHeader + payload) & 0xffffffff
	isCorrupt = (checksumInPacket != checksumCalculated)

	return packetType, sequenceNumber, packetLength, checksumInPacket, checksumCalculated, payload, isCorrupt



def main():
	if len(sys.argv) == 4:
		# read the command line arguments
		receiver_port = sys.argv[1]
		output_file = sys.argv[2]
		log_file = sys.argv[3]
	else:
		print("Usage: python MTPReceiver.py <receiver_port> <output_file> <log_file>")
		sys.exit(1)

	# open files and socket
	open(log_file, 'w').close()
	log = open(log_file, 'a')
	output = open(output_file, 'wb')
	server = socket(AF_INET, SOCK_DGRAM)
	server.bind(('', int(receiver_port)))
	#Added a Non-blocking recv with timeout
	server.settimeout(0.1) 
	
	print(f"Receiver listening on port {receiver_port}...")
	
	#Next expected sequence number
	expected_seq = 0
	#Out-of-order packets: {seq_num: data}
	buffer = {}
	#used to note the last ACK we sent
	last_ack_sent = -1
	ack_timer = None
	pending_ack_seq = None
	# Will be set when first packet arrives
	sender_addr = None
	lock = threading.RLock()
	#Will be used to stop running if enough time passed without getting a packet
	IDLE_EXIT_TIME = 2.0
	received_a_packet = False
	last_packet_time = None

	
	#We added a helper method to help with Ack's sending and receiver log outputs
	def send_ack(seq_num, addr):
		# Send ACK for seq_num
		nonlocal last_ack_sent
		if addr is None:
			return
		ack_packet = create_ack_packet(seq_num)
		packetType, ackSeq, ackLength, ackChecksum, _, _, _ = extract_packet_info(ack_packet)
		try:
			unreliable_channel.send_packet(server, ack_packet, addr)
			last_ack_sent = seq_num
			log.write(f"Packet sent; type=ACK; seqNum={ackSeq}; " f"length={ackLength}; checksum_in_packet={ackChecksum:08x}; status=NOT_CORRUPT\n")
			log.flush()
		except:
			pass

	#Added helper methods to help the handling of delayed acks 
	def cancel_delayed_ack():
		nonlocal ack_timer, pending_ack_seq
		with lock:
			if ack_timer is not None:
				ack_timer.cancel()
				ack_timer = None
			pending_ack_seq = None
	
	def delayed_ack_callback():
		nonlocal ack_timer, pending_ack_seq
		with lock:
			if pending_ack_seq is None:
				ack_timer = None
				return
			seq_to_ack = pending_ack_seq
			pending_ack_seq = None
			ack_timer = None
		send_ack(seq_to_ack, sender_addr)
		
	#Main logic
	#Corruption checks, ack generation, output file writes, buffering and shutdown.
	try:
		while True:
			try:
				packet_data, sender_addr = unreliable_channel.recv_packet(server)
				received_a_packet = True
				last_packet_time = time.time()
			#We added timeout shutdown checks so that receiver does not run forever once file transfer is complete
			except timeout:
				with lock:
					transfer_looks_complete = (received_a_packet and sender_addr is not None and len(buffer) == 0 and pending_ack_seq is None and last_ack_sent == expected_seq and last_packet_time is not None and (time.time() - last_packet_time) >= IDLE_EXIT_TIME)
				if transfer_looks_complete:
					log.write("Receiver idle after completed transfer; shutting down\n")
					log.flush()
					break
				continue
			
			packet_info = extract_packet_info(packet_data)
			packetType, sequenceNumber, packetLength, checksumInPacket, checksumCalculated, payload, isCorrupt = packet_info
			
			#Added corruption handling aswell as logging
			#We also cancel delayed acks, and ack the next sequence number
			if isCorrupt:
				log.write(
				f"Packet received; type={'DATA' if packetType == DATA else packetType}; "
				f"seqNum={sequenceNumber}; length={packetLength}; "
				f"checksum_in_packet={checksumInPacket if checksumInPacket is not None else 'None'}; "
				f"checksum_calculated={checksumCalculated if checksumCalculated is not None else 'None'}; "
				f"status=CORRUPT\n"
				)
				log.flush()
				cancel_delayed_ack()
				send_ack(expected_seq, sender_addr)
				continue
			
			if packetType != DATA:
				continue
			
			#Added receiver cases that were hinted at in the original skeleton
			with lock:
				# In-order packet
				if sequenceNumber == expected_seq:
					log.write(
						f"Packet received; type=DATA; seqNum={sequenceNumber}; "
						f"length={packetLength}; checksum_in_packet={checksumInPacket:08x}; "
						f"checksum_calculated={checksumCalculated:08x}; status=NOT_CORRUPT\n"
					)
					log.flush()

					output.write(payload)
					output.flush()
					expected_seq += 1
					
					advanced_from_buffer = False
					while expected_seq in buffer:
						buffered_payload = buffer.pop(expected_seq)
						output.write(buffered_payload)
						output.flush()
						expected_seq += 1
						advanced_from_buffer = True
					if advanced_from_buffer:
						cancel_delayed_ack()
						send_ack(expected_seq, sender_addr)
					elif ack_timer is None:
						pending_ack_seq = expected_seq
						ack_timer = threading.Timer(0.5, delayed_ack_callback)
						ack_timer.start()
					else:
						cancel_delayed_ack()
						send_ack(expected_seq, sender_addr)
				
				#Out of order packet
				elif sequenceNumber > expected_seq:
					#Added buffering for out of order packets  and immediate duplicate acks
					log.write(
						f"Packet received; type=DATA; seqNum={sequenceNumber}; "
						f"length={packetLength}; checksum_in_packet={checksumInPacket:08x}; "
						f"checksum_calculated={checksumCalculated:08x}; status=OUT_OF_ORDER_PACKET\n"
					)
					log.flush()

					if sequenceNumber not in buffer:
						buffer[sequenceNumber] = payload
					cancel_delayed_ack()
					send_ack(expected_seq, sender_addr)
				
				#Duplicate/old packet
				else:
					log.write(
						f"Packet received; type=DATA; seqNum={sequenceNumber}; "
						f"length={packetLength}; checksum_in_packet={checksumInPacket:08x}; "
						f"checksum_calculated={checksumCalculated:08x}; status=DUPLICATE_PACKET\n"
					)
					log.flush()

					cancel_delayed_ack()
					send_ack(expected_seq, sender_addr)
	#cleanup for when closing includes timers files and sockets
	finally:
		cancel_delayed_ack()
		output.close()
		log.close()
		server.close()
		print("Receiver shutdown")

if __name__ == "__main__":
	main()