# ECS 152A Programming Project 3 (Fall 2025)

This project is meant to teach TCP congestion control algorithms through implementation. You will build sender programs that transfer files over an emulated network with variable bandwidth, latency, and packet loss.

## Quick Start

### For Students

1. **Setup** (one time): Install Docker - see [SETUP.md](SETUP.md)
3. **Testing**: Use the simplified test script:
   ```bash
   cd docker
   ./test_sender.sh your_sender.py
   ```
4. **Troubleshooting**: Having issues? Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md)

### Quick Test Workflow

**macOS/Linux:**
```bash
cd docker

# Start the simulator (first time or if not running)
./start-simulator.sh

# Test your implementation
./test_sender.sh my_stop_and_wait.py
```

**Windows (Command Prompt/Batch):**
```batch
cd docker

# Start the simulator
start_sim.bat

# Test your implementation
test_sender.bat my_stop_and_wait.py
```

**The script will:**
- ✓ Check Docker is running
- ✓ Restart receiver to reset state
- ✓ Copy your code into container
- ✓ Run your sender
- ✓ Show performance metrics

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
├── test_sender.sh          # Test your sender - Bash (macOS/Linux)
├── test_sender.bat         # Test your sender - Batch (Windows)
├── receiver.py             # Pre-built receiver (DO NOT MODIFY)
├── training_profile.sh     # Network emulation script (DO NOT MODIFY)
├── docker-script.sh        # Container startup script
├── Dockerfile              # Container configuration
└── hdd/
    ├── file.mp3            # File to transfer (~5.1 MB)
```

## Network Simulation

The emulated network randomly transitions through 5 phases with varying conditions to test robustness and adaptability:

| Phase | Scenario | Bandwidth | Latency | Loss |
|-------|----------|-----------|---------|------|
| 1     | Stable Network | 45-55 Mbps | 40-70ms | 0-0.3% |
| 2     | Congestion | 8-13 Mbps | 60-100ms | 0.5-2% |
| 3     | High Latency | 25-40 Mbps | 100-250ms | 0-1% |
| 4     | Bursty Loss | 35-45 Mbps | 50-100ms | 0-20% |
| 5     | Reordering | 40-55 Mbps | 30-70ms | 0-0.5% |

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
