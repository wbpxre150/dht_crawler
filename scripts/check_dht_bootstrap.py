#!/usr/bin/env python3
"""
Probe candidate DHT bootstrap hosts.

For each host:port, send a single BEP5 ping datagram and wait briefly for
a BEP5 "pong" reply (matching the source IP/port we pinged). Report:
  - DNS resolution (v4/v6)
  - UDP reachability (whether we receive any reply at all)
  - Whether the reply is a valid BEP5 pong from the same host:port

Tunable via env:
  BOOTSTRAP_TIMEOUT (sec, default 3.0)
  BOOTSTRAP_TARGET  (default 1000)
"""
import os
import socket
import struct
import sys
import time
import bencode  # type: ignore

# Built-in candidates (mirrors + extends the lists in supervisor.c / thread_tree.c).
CANDIDATES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("dht.libtorrent.org", 25401),
    ("dht.aelitis.com", 6881),
    ("router.bitcomet.com", 6881),
    ("dht.anacrolix.link", 42069),
    ("router.utorrent.com", 6881),
    ("dht.vuze.com", 6881),
    ("router.bittorrent.cloud", 42069),
    ("router.silotis.us", 6881),
]

TIMEOUT = float(os.environ.get("BOOTSTRAP_TIMEOUT", "3.0"))


def make_ping(node_id: bytes, target: bytes) -> bytes:
    """BEP5 ping: d1:ad2:id20:<node>e1:q4:ping1:t2:aa1:y1:qe"""
    return (
        b"d1:ad2:id20:" + node_id +
        b"9:info_hash20:" + target +
        b"e1:q4:ping1:t2:aa1:y1:qe"
    )


def make_ping_no_infohash(node_id: bytes) -> bytes:
    """BEP5 ping without the info_hash trick: d1:ad2:id20:<node>e1:q4:ping1:t2:aa1:y1:qe"""
    return (
        b"d1:ad2:id20:" + node_id +
        b"e1:q4:ping1:t2:aa1:y1:qe"
    )


def resolve(host: str) -> list[tuple[int, str, int]]:
    out = []
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            infos = socket.getaddrinfo(host, None, family=family, type=socket.SOCK_DGRAM)
        except socket.gaierror:
            continue
        for fam, _, _, _, sa in infos:
            out.append((fam, sa[0], sa[1] if len(sa) > 1 else 0))
    return out


def probe(host: str, port: int) -> None:
    addrs = resolve(host)
    if not addrs:
        print(f"  {host}:{port}  DNS_FAIL")
        return
    for fam, ip, _ in addrs:
        tag = "v4" if fam == socket.AF_INET else "v6"
        sock = socket.socket(fam, socket.SOCK_DGRAM)
        sock.settimeout(TIMEOUT)
        try:
            node_id = os.urandom(20)
            pkt = make_ping_no_infohash(node_id)
            sock.sendto(pkt, (ip, port))
            t0 = time.time()
            try:
                data, addr = sock.recvfrom(2048)
                dt = time.time() - t0
                try:
                    decoded = bencode.decode(data)
                    print(f"  {host}:{port}  {tag}  {ip}  PONG  {dt*1000:.0f}ms  {len(data)}B  decoded={decoded!r}")
                except Exception:
                    print(f"  {host}:{port}  {tag}  {ip}  REPLY  {dt*1000:.0f}ms  {len(data)}B  raw={data!r}")
            except socket.timeout:
                print(f"  {host}:{port}  {tag}  {ip}  TIMEOUT_{int(TIMEOUT*1000)}ms")
        except OSError as e:
            print(f"  {host}:{port}  {tag}  {ip}  SEND_FAIL  {e}")
        finally:
            sock.close()


def main() -> int:
    print(f"# BOOTSTRAP_TIMEOUT={TIMEOUT}s")
    for h, p in CANDIDATES:
        probe(h, p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
