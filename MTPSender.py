## The code provided here is just a skeleton that can help you get started
## You can add/remove functions as you wish


## import (add more if you need)
import threading
import unreliable_channel
import zlib
import sys
import random
import struct
from socket import *
import time

#In unreliable_channel we had to change received_data+= "corrupted!" to received_data+= b"corrupted!"
#We needed to make it into byte type instead of it being text

## define and initialize
# window_size, window_base, next_seq_number, dup_ack_count, etc.
# client_port

#Added variables to help with handling sliding window, transmision time, packet list, and flag for completion
DATA = 0
ACK = 1
HEADER_FORMAT = "!IIII"
HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472
#Results in 1456
MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE 
#SAME AS 500MS
TIMEOUT = 0.5 

window_size = 0
window_base = 0
next_seq_number = 0
dup_ack_count = 0
last_ack_num = None
timer_start = None
packets = []
transfer_done = False
receiver_addr = None
log_handle = None

## we will need a lock to protect against concurrent threads
lock = threading.Lock()

#We kept the skeleton provided helper method for creating packets and added logic needed for it to run
def create_packet(packetType, sequenceNumber, payload=b""):
	packetLength = HEADER_SIZE + len(payload)
	tempHeader = struct.pack(HEADER_FORMAT, packetType, sequenceNumber, packetLength, 0)
	checksum = zlib.crc32(tempHeader + payload) & 0xffffffff
	finalHeader = struct.pack(HEADER_FORMAT, packetType, sequenceNumber, packetLength, checksum)
	return finalHeader + payload

# Two types of packets, data and ack
# crc32 available through zlib library


#We kept the skeleton provided helper method for extracting packet information and added the logic for it to run
#To do this We added packet parsing logic to validate incoming packets and detect corruption
def extract_packet_info(packetBytes):
	if len(packetBytes) < HEADER_SIZE:
		return None, None, None, None, None, b"", True
		#Handle it as corrupt

	headerBytes = packetBytes[:16]
	packetType, sequenceNumber, packetLength, checksumInPacket, = struct.unpack(HEADER_FORMAT, headerBytes)
	
	if packetLength < HEADER_SIZE or packetLength != len(packetBytes):
		return packetType, sequenceNumber, packetLength, checksumInPacket, None, b"", True
		#reject

	payload = packetBytes[HEADER_SIZE:packetLength]
	recomputedHeader = struct.pack(HEADER_FORMAT, packetType, sequenceNumber, packetLength, 0)
	checksumCalculated = zlib.crc32(recomputedHeader + payload) & 0xffffffff
	isCorrupt = (checksumInPacket != checksumCalculated)

	return packetType, sequenceNumber, packetLength, checksumInPacket, checksumCalculated, payload, isCorrupt
# extract the packet data after receiving

#Kept skeleton provided method and added the logic for it to run
#We added ACK processing so sender can keep transmitting
def receive_thread(client_socket):
	global window_base
	global dup_ack_count
	global last_ack_num
	global timer_start
	global transfer_done
	while not transfer_done:
		try:
			packet_data, server_addr = unreliable_channel.recv_packet(client_socket)
		except timeout:
			continue
		
		packetType, sequenceNumber, packetLength, checksumInPacket, checksumCalculated, payload, isCorrupt = extract_packet_info(packet_data)
		if isCorrupt:
			log_handle.write(
				f"Packet received; type={'ACK' if packetType == ACK else packetType}; "
				f"seqNum={sequenceNumber}; length={packetLength}; "
				f"checksum_in_packet={checksumInPacket if checksumInPacket is not None else 'None'}; "
				f"checksum_calculated={checksumCalculated if checksumCalculated is not None else 'None'}; "
				f"status=CORRUPT\n"
			)
			log_handle.flush()
			continue

		if packetType != ACK:
			continue

		log_handle.write(
			f"Packet received; type=ACK; seqNum={sequenceNumber}; "
			f"length={packetLength}; checksum_in_packet={checksumInPacket:08x}; "
			f"checksum_calculated={checksumCalculated:08x}; status=NOT_CORRUPT\n"
		)
		log_handle.flush()

		#handling ACks to update windows, handle dups 
		with lock:
			if sequenceNumber > window_base:
				window_base = sequenceNumber
				dup_ack_count = 0
				last_ack_num = sequenceNumber
				if window_base < next_seq_number:
					timer_start = time.time()
				else:
					timer_start = None
				log_window()

			elif sequenceNumber == window_base:
				if sequenceNumber == last_ack_num:
					dup_ack_count += 1
				else:
					last_ack_num = sequenceNumber
					dup_ack_count = 1

				if dup_ack_count == 3:
					log_handle.write(f"Triple dup acks received for packet seqNum={window_base}\n")
					log_handle.flush()
					if window_base < len(packets):
						unreliable_channel.send_packet(client_socket, packets[window_base], receiver_addr)
						timer_start = time.time()
					dup_ack_count = 0
			else:
				continue
		# receive packet, but using our unreliable channel
		# packet_from_server, server_addr = unreliable_channel.recv_packet(socket)
		# call extract_packet_info
		# check for corruption, take steps accordingly
		# update window size, timer, triple dup acks

#We added a helper method to help with loging sender window
def log_window():
	global window_base
	global next_seq_number
	global window_size
	global packets
	global log_handle

	window_entries = []
	window_end = min(window_base + window_size, len(packets))

	for seq in range(window_base, window_end):
		status_bit = 0 if seq < next_seq_number else 1
		window_entries.append(f"{seq}({status_bit})")
	log_handle.write("Updating window\n")
	log_handle.write(f"Window state: [{', '.join(window_entries)}]\n")
	log_handle.flush()

def main():
	global window_size
	global receiver_addr
	global packets
	global log_handle
	global next_seq_number
	global timer_start
	global transfer_done

	#We check to see if the correct amount of arguments were provided if not we advise the user and exit
	if len(sys.argv) != 6:
		print("Please pass in 5 arguments")
		sys.exit()
	#If the correct amount of arguments were given we start proccesing
	print("Starting Process")
	# read the command line arguments and save them into variables
	receiverIP = sys.argv[1]
	receiverPort = int(sys.argv[2])
	window_size = int(sys.argv[3])
	inputFile = sys.argv[4]
	senderLogFile = sys.argv[5]
	receiver_addr = (receiverIP, receiverPort)

	# open files
	with open(inputFile, "rb") as f:
		fileBytes = f.read()

	log_handle = open(senderLogFile, "w")
	# open client socket and bind
	client_socket = socket(AF_INET, SOCK_DGRAM)
	client_socket.settimeout(0.1)
	client_socket.bind(('', 0))
	
	# take the input file and split it into packets (use create_packet)
	seq_num = 0
	for start in range(0, len(fileBytes), MAX_PAYLOAD_SIZE):
		chunk = fileBytes[start:start + MAX_PAYLOAD_SIZE]
		packet = create_packet(DATA, seq_num ,chunk)
		packets.append(packet)
		seq_num += 1

	# start receive thread
	recv_thread = threading.Thread(target=receive_thread,args=(client_socket,))
	recv_thread.start()

	# while there are packets to send:
			# send packets to server using our unreliable_channel.send_packet() 
			# update the window size, timer, etc.
	while window_base < len(packets):
		with lock:
			sent_any = False
			while next_seq_number < len(packets) and next_seq_number < window_base + window_size:
				packetType, sequenceNumber, packetLength, checksumInPacket, _, _, _, = extract_packet_info(packets[next_seq_number])
				unreliable_channel.send_packet(client_socket, packets[next_seq_number], receiver_addr)
				log_handle.write(f"Packet sent; type=DATA; seqNum={sequenceNumber}; length={packetLength}; checksum={checksumInPacket:08x}\n")
				log_handle.flush()
				if next_seq_number == window_base:
					timer_start = time.time()
				next_seq_number += 1
				sent_any = True
			if sent_any:
				log_window()

			if timer_start is not None and time.time() - timer_start >= TIMEOUT:
				packetType, sequenceNumber, packetLength, checksumInPacket, _, _, _, = extract_packet_info(packets[window_base])
				unreliable_channel.send_packet(client_socket, packets[window_base], receiver_addr)
				log_handle.write(f"Timeout for packet seqNum={sequenceNumber}\n")
				log_handle.flush()
				timer_start = time.time()
	transfer_done = True
	recv_thread.join()

	log_handle.close()
	client_socket.close()

if __name__ == "__main__":
    main()