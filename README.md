# Reliable UDP Transport

A reliable file-transfer protocol implemented in Python on top of UDP. The project adds reliability mechanisms normally associated with transport protocols, including sequencing, acknowledgments, retransmissions, integrity checking, and window-based transmission.

## Features

- Transfers files over UDP sockets
- Uses sequence numbers to track transmitted data
- Implements a configurable sliding transmission window
- Processes acknowledgments from the receiver
- Retransmits packets when delivery fails
- Detects corrupted data using CRC32 checksums
- Handles duplicate and out-of-order packets
- Logs sender and receiver activity
- Supports testing over an unreliable simulated network channel

## Networking Concepts

This project demonstrates:

- UDP socket programming
- Reliable data transfer
- Sliding-window protocols
- Sequence numbers and acknowledgments
- Packet retransmission
- Timeout handling
- Error detection with CRC32
- Packet loss and corruption simulation

## Project Files

- `MTPSender.py` — sends a file using the reliable transport protocol
- `MTPReceiver.py` — receives and reconstructs the transferred file
- `unreliable_channel.py` — simulates unreliable network behavior
- `sample-input.txt` — small file for testing transfers

## Usage

Start the receiver first:

    python3 MTPReceiver.py <receiver-port> <output-file> <receiver-log>

Example:

    python3 MTPReceiver.py 5001 output.txt receiver-log.txt

Then start the sender:

    python3 MTPSender.py <receiver-IP> <receiver-port> <window-size> <input-file> <sender-log>

Example on the same machine:

    python3 MTPSender.py 127.0.0.1 5001 64 sample-input.txt sender-log.txt

When running the sender and receiver on different machines, replace `127.0.0.1` with the receiver machine's IP address.

## Contributors

- Juan Carlos Garcia Solis
- Emily Marie Hansen

This project was completed collaboratively as part of a university computer networking course.
