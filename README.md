# CScribe
A Scribe multicast P2P network build on top of Chord.


## Use:
**Download the project:**
```bash
git clone https://gitlab.au.dk/iot-golf/chord4convos.git
cd chord4convos
```

**Bootstrap the network:** Run a peer on any port (here 8000) and with the connection port being `0`.
```bash
go run . 8000 0
```

**Connect to an existing network:** Run a peer on any port (here 8001) with the connection port being the port of a peer in an existing network.
```bash
go run . 8001 8000
```

**Connect multiple peers to an existing network:** This is a testing tool, allowing you to connect any amount of peers (here 50) to the network at once. You should provice the port of the first peer, and make sure the following ports (here the following 50) are free.
```bash
go run . test 50 8001
```
