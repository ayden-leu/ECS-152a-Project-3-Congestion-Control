# Troubleshooting Guide

Common issues and their solutions for the ECS 152A Congestion Control assignment.

## Docker Issues

### Docker daemon not running

**Symptoms:**
```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock
```

**Solutions:**

**macOS:**
- If using Docker Desktop: Start Docker Desktop from Applications
- If using Colima: Run `colima start`

**Linux:**
```bash
sudo systemctl start docker
sudo systemctl enable docker  # Start on boot
```

**Windows:**
- Start Docker Desktop from the Start menu
- Ensure WSL 2 is installed and enabled

### Permission denied errors (Linux)

**Symptoms:**
```
Got permission denied while trying to connect to the Docker daemon socket
```

**Solution:**
```bash
# Add your user to the docker group
sudo usermod -aG docker $USER

# Apply the new group membership
newgrp docker

# Log out and log back in for permanent effect
```

### Container not found

**Symptoms:**
```
Error: No such container: ecs152a-simulator
```

**Solution:**
```bash
# Start the simulator
cd docker
./start-simulator.sh
```

### Container exists but won't start

**Solution:**
```bash
# Remove the old container and start fresh
docker rm -f ecs152a-simulator
./start-simulator.sh
```

## Network/Connection Issues

### Port 5001 already in use

**Symptoms:**
```
Bind for 0.0.0.0:5001 failed: port is already allocated
```

**Find what's using the port:**

**macOS/Linux:**
```bash
lsof -i :5001
# or
sudo netstat -tulpn | grep 5001
```

**Windows:**
```bash
netstat -ano | findstr :5001
```

**Solutions:**
1. Kill the process using the port
2. Or stop any other simulators running: `docker stop ecs152a-simulator`

### Receiver not responding

**Symptoms:**
- Sender times out waiting for ACKs
- No response from receiver

**Diagnostic steps:**
```bash
# Check if container is running
docker ps

# Check receiver logs
docker logs ecs152a-simulator

# Restart the simulator
docker restart ecs152a-simulator
```

**Common causes:**
- Receiver hasn't started yet (wait 3-5 seconds after starting simulator)
- Firewall blocking localhost connections
- Sending to wrong port (should be 5001)

### Connection refused

**Symptoms:**
```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Solutions:**
1. Ensure receiver is running: `docker ps`
2. Check you're connecting to correct address: `localhost:5001`
3. Restart the simulator: `./start-simulator.sh`


## Testing Issues

### test_sender.sh: command not found (macOS/Linux)

**Solution:**
```bash
# Make script executable
chmod +x test_sender.sh

# Run with ./
./test_sender.sh my_sender.py
```
```

### Path issues on Windows

**Symptom:**
```
file.mp3 not found
```

**Solution:**
- Use backslashes on Windows: `hdd\file.mp3` not `hdd/file.mp3`
- Or run scripts from the `docker` directory:
  ```batch
  cd docker
  test_sender.bat my_sender.py
  ```

### "Docker not in PATH" error

**Solution:**
```bash
# Check if Docker is installed
which docker

# If installed but not in PATH, add to PATH
export PATH="/usr/local/bin:$PATH"  # macOS/Linux

# Or restart terminal after Docker installation
```

### Script fails to copy file into container

**Cause:** Container not running

**Solution:**
```bash
# Check container status
docker ps -a

# Start container if stopped
docker start ecs152a-simulator

# Or restart simulator
./start-simulator.sh
```

## Debugging Strategies

### Add verbose logging

```python
DEBUG = True

if DEBUG:
    print(f"Sent packet {seq_id}, window [{base}:{base+cwnd}]")
    print(f"Received ACK {ack_seq_id}")
    print(f"cwnd={cwnd:.2f}, ssthresh={ssthresh}, dup_acks={duplicate_acks}")
```

### Test with smaller file first

Replace file.mp3 with a smaller test file to iterate faster:
```bash
# Create small test file
head -c 10000 file.mp3 > test_small.mp3
docker cp test_small.mp3 ecs152a-simulator:/hdd/file.mp3
```

### Check receiver logs

```bash
docker logs ecs152a-simulator
```

Shows:
- Which packets were received
- Current network phase
- Any errors from receiver

### Manual testing

Instead of using test script:
```bash
# Start simulator
./start-simulator.sh

# In another terminal, copy and run manually
docker cp my_sender.py ecs152a-simulator:/app/
docker exec ecs152a-simulator python3 /app/my_sender.py
```

### Verify packet format

```python
# Print packet contents
packet = create_packet(seq_id, message)
print(f"Packet: seq_id={seq_id}, size={len(packet)}")
print(f"  Seq bytes: {packet[:4].hex()}")
print(f"  Payload size: {len(packet[4:])}")
```

## Still Having Issues?

1. **Review ASSIGNMENT.md** - Check protocol specification
2. **Compare with discussion code** - Week 7 receiver example
3. **Post on discussion board** - Include error messages and what you've tried
4. **Office hours** - Bring specific questions and code snippets

## Quick Reference Commands

```bash
# Start simulator
./start-simulator.sh

# Test your sender
./test_sender.sh my_sender.py

# Check Docker status
docker ps

# View receiver logs
docker logs ecs152a-simulator

# Restart receiver
docker restart ecs152a-simulator

# Stop simulator
docker stop ecs152a-simulator

# Remove container completely
docker rm -f ecs152a-simulator

# Copy file from container
docker cp ecs152a-simulator:/hdd/file2.mp3 ./received.mp3

# Run commands inside container
docker exec ecs152a-simulator ls -la /hdd/
```
