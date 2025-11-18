# ECS 152A Programming Project 3 (Fall 2025)

This project is meant to teach TCP congestion control algorithms through implementation. You will build sender programs that transfer files over an emulated network with variable bandwidth, latency, and packet loss.

## Quick Start

1. **Setup** (one time): Install Docker - see [SETUP.md](SETUP.md)
2. **Testing**: Use the simplified test script (optionally pass a custom payload):
   ```bash
   cd docker
   ./test_sender.sh your_sender.py [payload.zip]
   ```
3. **Troubleshooting**: Having issues? Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md)

### Quick Test Workflow

**macOS/Linux:**

```bash
cd docker

# Build + launch simulator (detached, keeps running)
./start-simulator.sh

# Test your implementation (optional payload arg)
./test_sender.sh my_stop_and_wait.py [payload.zip]
```

**Windows (Command Prompt/Batch):**

```batch
cd docker

# Build + launch simulator (detached, keeps running)
start_sim.bat

# Test your implementation (optional payload arg)
test_sender.bat my_stop_and_wait.py [payload.zip]
```

**What the test scripts now do:**

- ✓ Verify Docker/daemon status
- ✓ Ensure the long-running simulator container is up
- ✓ Copy your sender and the chosen payload into `/app` and `/hdd`
- ✓ Start a fresh in-container receiver for every run (instead of restarting the container)
- ✓ Run your sender, collect metrics, and print CSV + averages

Need to compare two senders side-by-side? Use the updated fairness harness:

```bash
./test_fairness.sh senderA.py senderB.py [payload.zip]
```
It launches two receivers (ports 5001/5002) inside the existing simulator, runs both senders concurrently, and reports Jain’s fairness index plus averaged metrics.

### Payload files 101

- **Default file**: Every script uses `file.zip` (from `docker/` or `docker/hdd/`) when you omit the third argument.
- **Custom file**: Pass any relative or absolute path (`./test_sender.sh my_sender.py assets/mytrace.zip`). The script copies it into `/hdd/` inside the container with the same basename.
- **Receivers**: The test harness sets `PAYLOAD_FILE`/`TEST_FILE` env vars before starting in-container receivers. The stock `receiver.py` and the provided Python sender templates consume these to find both the source file (`/hdd/<name>`) and the output (`/hdd/<name>_received.*`).
- **Fairness tests**: `test_fairness.sh senderA.py senderB.py custom.zip` uses the exact same file for both flows, so you can benchmark with different dataset sizes without editing the scripts.

Tip: Store any extra payloads under `docker/hdd/` so they’re volume-mounted automatically, then invoke the scripts with just the filename.

### Skeleton sender for smoke testing

Need to verify Docker, the simulator, and the receiver are all behaving before you start coding? Use the baked-in skeleton:

```bash
cd docker
./test_sender.sh sender_skeleton.py [payload.zip]
```

It sends two quick demo packets (plus the EOF marker) and prints metrics so you can confirm the end-to-end flow. Feel free to copy `docker/sender_skeleton.py` as a starting point for your own sender; it already demonstrates:

- how to read `PAYLOAD_FILE` / `TEST_FILE` from the environment
- how to format packets (`SEQ_ID` + payload) and parse ACK/FIN responses
- how to emit the CSV metrics line that the grading scripts expect

## What You'll Implement

Four congestion control algorithms:

1. **Stop-and-Wait** - Send one packet, wait for ACK (simplest)
2. **TCP Tahoe** - Slow start + congestion avoidance + fast retransmit
3. **TCP Reno** - Adds fast recovery to Tahoe for better performance
4. **Custom Protocol** - Design your own congestion control algorithm (optional)

## Documentation

- **[SETUP.md](SETUP.md)** - Docker installation guide for all platforms
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues and solutions

## Project Structure

```
docker/
├── start-simulator.sh      # Start the receiver and network emulator (macOS/Linux)
├── start_sim.bat           # Start the receiver and network emulator (Windows)
├── test_sender.sh          # Test your sender (optional payload) - Bash
├── test_sender.bat         # Test your sender (optional payload) - Batch
├── test_fairness.sh        # Compare two senders side by side
├── receiver.py             # Pre-built receiver (DO NOT MODIFY)
├── training_profile.sh     # Network emulation script (DO NOT MODIFY)
├── docker-script.sh        # Container startup script
├── Dockerfile              # Container configuration
└── hdd/
    ├── file.zip            # File to transfer (~5.1 MB)
```

## Network Simulation

The emulated network randomly transitions through 5 phases with varying conditions to test robustness and adaptability:

| Phase | Scenario       | Bandwidth  | Latency   | Loss   | Reordering |
| ----- | -------------- | ---------- | --------- | ------ | ---------- |
| 1     | Stable Network | 45-55 Mbps | 40-70ms   | 0-1%   | 0-0%       |
| 2     | Congestion     | 8-13 Mbps  | 60-100ms  | 0.5-2% | 0-0%       |
| 3     | High Latency   | 25-40 Mbps | 100-250ms | 0-1%   | 0-0%       |
| 4     | Bursty Loss    | 35-45 Mbps | 50-100ms  | 0-8%   | 0-0%       |
| 5     | Reordering     | 40-55 Mbps | 30-70ms   | 0-1%   | 1-5%       |

## Important Notes

⚠️ **You are NOT supposed to make changes to any file in this repository except your own sender implementations.**

Files you should NOT modify:

- `receiver.py` - Pre-built receiver
- `training_profile.sh` - Network emulation configuration
- `docker-script.sh` - Container startup logic
- `Dockerfile` - Container setup

## Getting Help

2. Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues
3. Review Week 7 discussion code: [receiver.py example](https://github.com/shaoormunir/ecs152a-fall-2025/blob/main/week7/code/receiver.py)
4. Post on course discussion board on Canvas
5. Attend office hours

## Additional Resources

- [TCP Congestion Control (RFC 5681)](https://datatracker.ietf.org/doc/html/rfc5681)
- [Docker Documentation](https://docs.docker.com/)

Good luck! 🚀
