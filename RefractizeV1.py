#!/usr/bin/env python3
"""
Decentralized P2P Chat - LAN Peer Discovery and Signaling Server
Combines UDP broadcast discovery with WebRTC signaling capabilities
"""

import asyncio
import json
import socket
import time
import uuid
import logging
import threading
from typing import Dict, Set, Optional, Tuple
from dataclasses import dataclass, asdict

import websockets
from websockets import serve
import websockets.exceptions


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PeerInfo:
    """Information about a discovered peer"""
    peer_id: str
    ip_address: str
    signaling_port: int
    discovery_port: int
    last_seen: float
    is_signaling_server: bool = False
    priority: int = 0  # Higher number = higher priority for signaling server election

class LANPeerDiscovery:
    """Handles peer discovery using UDP broadcast/multicast"""
    
    DISCOVERY_PORT = 7001
    BROADCAST_INTERVAL = 5.0  # seconds
    PEER_TIMEOUT = 15.0  # seconds
    ELECTION_TIMEOUT = 3.0  # seconds for signaling server election
    
    def __init__(self, peer_id: str = None, signaling_port: int = 8001):
        self.peer_id = peer_id or str(uuid.uuid4())
        self.signaling_port = signaling_port
        self.peers: Dict[str, PeerInfo] = {}
        self.is_running = False
        self.socket = None
        self.local_ip = self._get_local_ip()
        self.is_signaling_server = False
        self.priority = int(time.time() * 1000) % 10000  # Random priority based on startup time
        
    def _get_local_ip(self) -> str:
        """Get the local LAN IP address"""
        try:
            # Connect to a dummy address to determine local IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                return s.getsockname()[0]
        except Exception:
            return '127.0.0.1'
    
    async def start_discovery(self):
        """Start the peer discovery service"""
        self.is_running = True

        # Create UDP socket for broadcasting
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', self.DISCOVERY_PORT))
        self.socket.settimeout(1.0)

        logger.info(f"Peer {self.peer_id[:8]} started discovery on {self.local_ip}:{self.DISCOVERY_PORT}")

        # Immediately elect ourselves if no peers yet
        await self._elect_signaling_server()

        # Start listening and broadcasting tasks
        listen_task = asyncio.create_task(self._listen_for_peers())
        broadcast_task = asyncio.create_task(self._broadcast_presence())
        cleanup_task = asyncio.create_task(self._cleanup_stale_peers())

        await asyncio.gather(listen_task, broadcast_task, cleanup_task)

        async def _listen_for_peers(self):
            """Listen for peer discovery messages"""
            while self.is_running:
                try:
                    data, addr = self.socket.recvfrom(1024)
                    await self._handle_discovery_message(data, addr[0])
                except socket.timeout:
                    continue
                except Exception as e:
                    logger.error(f"Error receiving discovery message: {e}")
                    await asyncio.sleep(0.1)
    
    async def _handle_discovery_message(self, data: bytes, sender_ip: str):
        """Process incoming discovery messages"""
        try:
            message = json.loads(data.decode('utf-8'))
            
            if message.get('type') == 'peer_discovery':
                peer_data = message.get('peer_info')
                if not peer_data or peer_data.get('peer_id') == self.peer_id:
                    return  # Ignore our own messages
                
                peer_info = PeerInfo(
                    peer_id=peer_data['peer_id'],
                    ip_address=sender_ip,
                    signaling_port=peer_data['signaling_port'],
                    discovery_port=peer_data['discovery_port'],
                    last_seen=time.time(),
                    is_signaling_server=peer_data.get('is_signaling_server', False),
                    priority=peer_data.get('priority', 0)
                )
                
                # Update or add peer
                old_peer = self.peers.get(peer_info.peer_id)
                self.peers[peer_info.peer_id] = peer_info
                
                if not old_peer:
                    logger.info(f"Discovered new peer: {peer_info.peer_id[:8]} at {sender_ip}")
                    # Trigger signaling server election when new peer joins
                    await self._elect_signaling_server()
                
        except Exception as e:
            logger.error(f"Error handling discovery message: {e}")
    
    async def _broadcast_presence(self):
        """Broadcast our presence to the LAN"""
        while self.is_running:
            try:
                message = {
                    'type': 'peer_discovery',
                    'peer_info': {
                        'peer_id': self.peer_id,
                        'signaling_port': self.signaling_port,
                        'discovery_port': self.DISCOVERY_PORT,
                        'is_signaling_server': self.is_signaling_server,
                        'priority': self.priority
                    }
                }
                
                data = json.dumps(message).encode('utf-8')
                
                # Broadcast to LAN
                broadcast_ip = self._get_broadcast_address()
                self.socket.sendto(data, (broadcast_ip, self.DISCOVERY_PORT))
                
                await asyncio.sleep(self.BROADCAST_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error broadcasting presence: {e}")
                await asyncio.sleep(self.BROADCAST_INTERVAL)
    
    def _get_broadcast_address(self) -> str:
        """Calculate broadcast address for the local network"""
        # Simple approach: assume /24 network
        ip_parts = self.local_ip.split('.')
        return f"{'.'.join(ip_parts[:3])}.255"
    
    async def _cleanup_stale_peers(self):
        """Remove peers that haven't been seen recently"""
        while self.is_running:
            current_time = time.time()
            stale_peers = [
                peer_id for peer_id, peer in self.peers.items()
                if current_time - peer.last_seen > self.PEER_TIMEOUT
            ]
            
            for peer_id in stale_peers:
                logger.info(f"Removing stale peer: {peer_id[:8]}")
                del self.peers[peer_id]
            
            if stale_peers:
                await self._elect_signaling_server()
            
            await asyncio.sleep(5.0)
        
    async def _elect_signaling_server(self):
        """Elect a signaling server among all peers"""
        await asyncio.sleep(self.ELECTION_TIMEOUT)  # Wait for any discovery messages

        # Include ourselves in the election
        all_peers = list(self.peers.values())
        self_peer = PeerInfo(
            peer_id=self.peer_id,
            ip_address=self.local_ip,
            signaling_port=self.signaling_port,
            discovery_port=self.DISCOVERY_PORT,
            last_seen=time.time(),
            is_signaling_server=self.is_signaling_server,
            priority=self.priority
        )
        all_peers.append(self_peer)

        # Sort by priority (highest first), then peer_id
        all_peers.sort(key=lambda p: (-p.priority, p.peer_id))

        if not all_peers:
            # No peers at all — default to self
            self.is_signaling_server = True
            logger.info(f"Defaulting to self as signaling server (priority: {self.priority})")
            return

        elected_peer = all_peers[0]
        should_be_signaling_server = (elected_peer.peer_id == self.peer_id)

        if should_be_signaling_server != self.is_signaling_server:
            self.is_signaling_server = should_be_signaling_server
            if self.is_signaling_server:
                logger.info(f"Elected as signaling server (priority: {self.priority})")
            else:
                logger.info(f"No longer signaling server. New server: {elected_peer.peer_id[:8]}")

    def get_signaling_server(self) -> Optional[PeerInfo]:
        """Get the current signaling server peer"""
        if self.is_signaling_server:
            return PeerInfo(
                peer_id=self.peer_id,
                ip_address=self.local_ip,
                signaling_port=self.signaling_port,
                discovery_port=self.DISCOVERY_PORT,
                last_seen=time.time(),
                is_signaling_server=True,
                priority=self.priority
            )
        
        for peer in self.peers.values():
            if peer.is_signaling_server:
                return peer
        
        return None
    
    def stop(self):
        """Stop the discovery service"""
        self.is_running = False
        if self.socket:
            self.socket.close()


class WebRTCSignalingServer:
    """WebRTC signaling server for establishing peer connections"""
    
    def __init__(self, port: int = 8001):
        self.port = port
        self.clients: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.is_running = False
        
    async def start_server(self):
        """Start the WebRTC signaling server"""
        if self.is_running:
            return
        
        self.is_running = True
        logger.info(f"Starting WebRTC signaling server on port {self.port}")
        
        async with serve(self._handle_client, "0.0.0.0", self.port) as server:
            await server.wait_closed()
    
    async def _handle_client(self, websocket, path):
        """Handle new WebSocket client connections"""
        client_id = None
        try:
            async for message in websocket:
                data = json.loads(message)
                await self._process_message(websocket, data)
                
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {client_id[:8] if client_id else 'unknown'} disconnected")
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            if client_id and client_id in self.clients:
                del self.clients[client_id]
                await self._broadcast_peer_left(client_id)
    
    async def _process_message(self, websocket, data: dict):
        """Process incoming signaling messages"""
        message_type = data.get('type')
        
        if message_type == 'register':
            client_id = data.get('clientId')
            self.clients[client_id] = websocket
            logger.info(f"Client registered: {client_id[:8]}")
            
            # Send current peer list
            peer_list = [cid for cid in self.clients.keys() if cid != client_id]
            await websocket.send(json.dumps({
                'type': 'peer_list',
                'peers': peer_list
            }))
            
            # Notify other peers of new peer
            await self._broadcast_to_others(client_id, {
                'type': 'peer_joined',
                'peerId': client_id
            })
            
        elif message_type in ['offer', 'answer', 'ice_candidate']:
            target_id = data.get('targetId')
            sender_id = data.get('senderId')
            
            if target_id in self.clients:
                # Forward the message to the target peer
                await self.clients[target_id].send(json.dumps({
                    'type': message_type,
                    'senderId': sender_id,
                    'data': data.get('data')
                }))
    
    async def _broadcast_to_others(self, sender_id: str, message: dict):
        """Broadcast a message to all clients except the sender"""
        for client_id, websocket in self.clients.items():
            if client_id != sender_id:
                try:
                    await websocket.send(json.dumps(message))
                except Exception as e:
                    logger.error(f"Error broadcasting to {client_id[:8]}: {e}")
    
    async def _broadcast_peer_left(self, departed_id: str):
        """Notify all clients that a peer has left"""
        message = {
            'type': 'peer_left',
            'peerId': departed_id
        }
        
        for websocket in self.clients.values():
            try:
                await websocket.send(json.dumps(message))
            except Exception as e:
                logger.error(f"Error notifying peer departure: {e}")
    
    def stop(self):
        """Stop the signaling server"""
        self.is_running = False


class DecentralizedChatNode:
    """Main node that combines peer discovery and signaling capabilities"""
    
    def __init__(self, signaling_port: int = 8001):
        self.peer_id = str(uuid.uuid4())
        self.signaling_port = signaling_port
        
        self.discovery = LANPeerDiscovery(self.peer_id, signaling_port)
        self.signaling_server = WebRTCSignalingServer(signaling_port)
        
        self.is_running = False
    
    async def start(self):
        """Start the decentralized chat node"""
        self.is_running = True
        logger.info(f"Starting decentralized chat node: {self.peer_id[:8]}")
        
        # Start peer discovery
        discovery_task = asyncio.create_task(self.discovery.start_discovery())
        
        # Monitor signaling server status
        signaling_task = asyncio.create_task(self._manage_signaling_server())
        
        await asyncio.gather(discovery_task, signaling_task)
    
    async def _manage_signaling_server(self):
        """Manage signaling server based on election results"""
        signaling_server_task = None
        
        while self.is_running:
            try:
                should_run_server = self.discovery.is_signaling_server
                is_server_running = (signaling_server_task is not None and 
                                   not signaling_server_task.done())
                
                if should_run_server and not is_server_running:
                    # Start signaling server
                    logger.info("Starting signaling server...")
                    signaling_server_task = asyncio.create_task(
                        self.signaling_server.start_server()
                    )
                    
                elif not should_run_server and is_server_running:
                    # Stop signaling server
                    logger.info("Stopping signaling server...")
                    self.signaling_server.stop()
                    if signaling_server_task:
                        signaling_server_task.cancel()
                        try:
                            await signaling_server_task
                        except asyncio.CancelledError:
                            pass
                        signaling_server_task = None
                
                await asyncio.sleep(2.0)
                
            except Exception as e:
                logger.error(f"Error managing signaling server: {e}")
                await asyncio.sleep(5.0)
    
    def get_peer_info(self) -> dict:
        """Get information about this peer and discovered peers"""
        signaling_server = self.discovery.get_signaling_server()
        
        return {
            'self': {
                'peer_id': self.peer_id,
                'ip_address': self.discovery.local_ip,
                'signaling_port': self.signaling_port,
                'is_signaling_server': self.discovery.is_signaling_server,
                'priority': self.discovery.priority
            },
            'peers': {pid: asdict(peer) for pid, peer in self.discovery.peers.items()},
            'signaling_server': asdict(signaling_server) if signaling_server else None
        }
    
    def stop(self):
        """Stop the chat node"""
        self.is_running = False
        self.discovery.stop()
        self.signaling_server.stop()


async def main():
    """Main function to run the decentralized chat node"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Decentralized P2P Chat Node')
    parser.add_argument('--port', type=int, default=8001, 
                       help='Signaling server port (default: 8001)')
    parser.add_argument('--info-interval', type=int, default=10,
                       help='Interval to print peer info (default: 10 seconds)')
    
    args = parser.parse_args()
    
    node = DecentralizedChatNode(args.port)
    
    # Start node in background
    node_task = asyncio.create_task(node.start())
    
    # Periodically print peer information
    try:
        while True:
            await asyncio.sleep(args.info_interval)
            info = node.get_peer_info()
            
            print("\n" + "="*60)
            print(f"Peer ID: {info['self']['peer_id'][:8]}")
            print(f"Local IP: {info['self']['ip_address']}")
            print(f"Signaling Port: {info['self']['signaling_port']}")
            print(f"Is Signaling Server: {info['self']['is_signaling_server']}")
            print(f"Priority: {info['self']['priority']}")
            
            print(f"\nDiscovered Peers ({len(info['peers'])}):")
            for peer_id, peer_data in info['peers'].items():
                print(f"  {peer_id[:8]} - {peer_data['ip_address']}:{peer_data['signaling_port']} "
                     f"(Server: {peer_data['is_signaling_server']}, Priority: {peer_data['priority']})")
            
            if info['signaling_server']:
                server = info['signaling_server']
                print(f"\nActive Signaling Server: {server['peer_id'][:8]} "
                     f"at {server['ip_address']}:{server['signaling_port']}")
            else:
                print("\nNo active signaling server")
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        node.stop()
        node_task.cancel()
        try:
            await node_task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    asyncio.run(main())