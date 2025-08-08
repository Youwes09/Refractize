#!/usr/bin/env python3
"""
Improved Decentralized P2P Chat - Fixed Signaling Port with Single Server Election
Key improvements:
- Fixed signaling port (8001) for all nodes
- Only one elected server runs at a time
- Clear separation between discovery and signaling roles
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
from websockets import serve
import websockets.exceptions

# Global constants
SIGNALING_PORT = 8001  # Fixed port for all signaling servers
DISCOVERY_PORT = 7001  # Fixed port for peer discovery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PeerInfo:
    """Information about a discovered peer"""
    peer_id: str
    ip_address: str
    last_seen: float
    priority: int = 0  # Higher number = higher priority for signaling server election
    
    def is_stale(self, timeout: float = 15.0) -> bool:
        """Check if peer hasn't been seen recently"""
        return time.time() - self.last_seen > timeout

class LANPeerDiscovery:
    """Handles peer discovery using UDP broadcast and signaling server election"""
    
    BROADCAST_INTERVAL = 5.0  # seconds
    PEER_TIMEOUT = 15.0  # seconds
    ELECTION_DELAY = 2.0  # seconds to wait before election
    
    def __init__(self, peer_id: str = None, signaling_port: int = 8001):
        self.peer_id = peer_id or str(uuid.uuid4())
        self.signaling_port = signaling_port
        self.peers: Dict[str, PeerInfo] = {}
        self.is_running = False
        self.socket = None
        self.local_ip = self._get_local_ip()
        
        # Signaling server election state
        self.elected_signaling_server_id = None
        self.priority = int(time.time() * 1000) % 10000
        
        # Callbacks for signaling server changes
        self.on_signaling_server_changed = None

        
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
        self.socket.bind(('', DISCOVERY_PORT))
        self.socket.settimeout(1.0)
        
        logger.info(f"Peer {self.peer_id[:8]} started discovery on {self.local_ip}:{DISCOVERY_PORT}")
        logger.info(f"Signaling server will use port {SIGNALING_PORT}")
        
        # Start tasks
        listen_task = asyncio.create_task(self._listen_for_peers())
        broadcast_task = asyncio.create_task(self._broadcast_presence())
        cleanup_task = asyncio.create_task(self._cleanup_stale_peers())
        
        # Initial election after short delay
        await asyncio.sleep(1.0)
        await self._elect_signaling_server()
        
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
                    last_seen=time.time(),
                    priority=peer_data.get('priority', 0)
                )
                
                # Check if this is a new peer
                is_new_peer = peer_info.peer_id not in self.peers
                
                # Update peer info
                self.peers[peer_info.peer_id] = peer_info
                
                if is_new_peer:
                    logger.info(f"Discovered new peer: {peer_info.peer_id[:8]} at {sender_ip} (priority: {peer_info.priority})")
                    # Trigger election when new peer joins
                    await asyncio.sleep(self.ELECTION_DELAY)
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
                        'priority': self.priority
                    }
                }
                
                data = json.dumps(message).encode('utf-8')
                
                # Broadcast to LAN
                broadcast_ip = self._get_broadcast_address()
                self.socket.sendto(data, (broadcast_ip, DISCOVERY_PORT))
                
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
                if peer.is_stale(self.PEER_TIMEOUT)
            ]
            
            signaling_server_left = False
            for peer_id in stale_peers:
                logger.info(f"Removing stale peer: {peer_id[:8]}")
                if peer_id == self.elected_signaling_server_id:
                    signaling_server_left = True
                del self.peers[peer_id]
            
            # Re-elect if signaling server left or if we had stale peers
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

        # Sort by priority (highest first), then peer_id ascending
        all_peers.sort(key=lambda p: (-p.priority, p.peer_id))

        # If only self is present, elect self immediately
        if len(all_peers) == 1 and all_peers[0].peer_id == self.peer_id:
            self.is_signaling_server = True
            logger.info(f"Defaulting to self as signaling server (priority: {self.priority})")
            return

        # Otherwise, elect highest priority peer
        elected_peer = all_peers[0]
        should_be_signaling_server = (elected_peer.peer_id == self.peer_id)

        if should_be_signaling_server != self.is_signaling_server:
            self.is_signaling_server = should_be_signaling_server
            if self.is_signaling_server:
                logger.info(f"Elected as signaling server (priority: {self.priority})")
            else:
                logger.info(f"No longer signaling server. New server: {elected_peer.peer_id[:8]}")

    
    def is_signaling_server(self) -> bool:
        """Check if this peer is the elected signaling server"""
        return self.elected_signaling_server_id == self.peer_id
    
    def get_signaling_server_info(self) -> Optional[Tuple[str, str]]:
        """Get the IP and port of the current signaling server"""
        if not self.elected_signaling_server_id:
            return None
            
        if self.elected_signaling_server_id == self.peer_id:
            return (self.local_ip, SIGNALING_PORT)
        
        peer = self.peers.get(self.elected_signaling_server_id)
        if peer:
            return (peer.ip_address, SIGNALING_PORT)
        
        return None
    
    def get_signaling_server_websocket_url(self) -> Optional[str]:
        """Get the WebSocket URL for the current signaling server"""
        server_info = self.get_signaling_server_info()
        if server_info:
            ip, port = server_info
            return f"ws://{ip}:{port}"
        return None
    
    def stop(self):
        """Stop the discovery service"""
        self.is_running = False
        if self.socket:
            self.socket.close()


class WebRTCSignalingServer:
    """WebRTC signaling server - only one instance runs across the network"""
    
    def __init__(self, port: int = 8001):
        self.port = port
        self.clients: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.is_running = False
        self.server = None
        
    async def start_server(self):
        """Start the WebRTC signaling server on the fixed port"""
        logger.info(f"Signaling server listening on port {self.port}")
        if self.is_running:
            logger.warning("Signaling server already running")
            return
        
        self.is_running = True
        logger.info(f"🚀 Starting WebRTC signaling server on port {SIGNALING_PORT}")
        
        try:
            self.server = await serve(
                self._handle_client, 
                "0.0.0.0", 
                SIGNALING_PORT,
                ping_interval=20,
                ping_timeout=10
            )
            
            logger.info(f"✅ Signaling server listening on 0.0.0.0:{SIGNALING_PORT}")
            await self.server.wait_closed()
            
        except Exception as e:
            logger.error(f"Error starting signaling server: {e}")
            self.is_running = False
            raise
    
    async def stop_server(self):
        """Stop the WebRTC signaling server"""
        if not self.is_running:
            return
            
        logger.info("🛑 Stopping WebRTC signaling server")
        self.is_running = False
        
        # Close all client connections
        if self.clients:
            logger.info(f"Closing {len(self.clients)} client connections")
            for client_id, websocket in list(self.clients.items()):
                try:
                    await websocket.close(code=1001, reason="Server shutting down")
                except:
                    pass
        
        self.clients.clear()
        
        # Stop the server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        
        logger.info("✅ Signaling server stopped")
    
    async def _handle_client(self, websocket, path):
        """Handle new WebSocket client connections"""
        client_id = None
        client_addr = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        
        try:
            logger.info(f"New client connection from {client_addr}")
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    result = await self._process_message(websocket, data, client_addr)
                    if result and 'clientId' in result:
                        client_id = result['clientId']
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON from client {client_addr}")
                except Exception as e:
                    logger.error(f"Error processing message from {client_addr}: {e}")
                
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {client_id[:8] if client_id else client_addr} disconnected")
        except Exception as e:
            logger.error(f"Error handling client {client_addr}: {e}")
        finally:
            if client_id and client_id in self.clients:
                del self.clients[client_id]
                await self._broadcast_peer_left(client_id)
    
    async def _process_message(self, websocket, data: dict, client_addr: str):
        """Process incoming signaling messages"""
        message_type = data.get('type')
        
        if message_type == 'register':
            client_id = data.get('clientId')
            if not client_id:
                logger.error(f"Registration without clientId from {client_addr}")
                return None
                
            self.clients[client_id] = websocket
            logger.info(f"📝 Client registered: {client_id[:8]} from {client_addr}")
            
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
            
            return {'clientId': client_id}
            
        elif message_type in ['offer', 'answer', 'ice_candidate']:
            target_id = data.get('targetId')
            sender_id = data.get('senderId')
            
            if not target_id or not sender_id:
                logger.error(f"Missing targetId or senderId in {message_type} from {client_addr}")
                return None
            
            if target_id in self.clients:
                # Forward the message to the target peer
                await self.clients[target_id].send(json.dumps({
                    'type': message_type,
                    'senderId': sender_id,
                    'data': data.get('data')
                }))
                logger.debug(f"Forwarded {message_type} from {sender_id[:8]} to {target_id[:8]}")
            else:
                logger.warning(f"Target peer {target_id[:8]} not found for {message_type}")
        
        else:
            logger.warning(f"Unknown message type '{message_type}' from {client_addr}")
        
        return None
    
    async def _broadcast_to_others(self, sender_id: str, message: dict):
        """Broadcast a message to all clients except the sender"""
        if not self.clients:
            return
            
        failed_clients = []
        for client_id, websocket in self.clients.items():
            if client_id != sender_id:
                try:
                    await websocket.send(json.dumps(message))
                except Exception as e:
                    logger.error(f"Error broadcasting to {client_id[:8]}: {e}")
                    failed_clients.append(client_id)
        
        # Clean up failed clients
        for client_id in failed_clients:
            if client_id in self.clients:
                del self.clients[client_id]
    
    async def _broadcast_peer_left(self, departed_id: str):
        """Notify all clients that a peer has left"""
        if not self.clients:
            return
            
        message = {
            'type': 'peer_left',
            'peerId': departed_id
        }
        
        logger.info(f"Broadcasting peer departure: {departed_id[:8]}")
        
        failed_clients = []
        for client_id, websocket in self.clients.items():
            try:
                await websocket.send(json.dumps(message))
            except Exception as e:
                logger.error(f"Error notifying peer departure to {client_id[:8]}: {e}")
                failed_clients.append(client_id)
        
        # Clean up failed clients
        for client_id in failed_clients:
            if client_id in self.clients:
                del self.clients[client_id]


class DecentralizedChatNode:
    """Main node that combines peer discovery and signaling capabilities"""
    
    def __init__(self, signaling_port: int = 8001, force_server: bool = False):
        self.peer_id = str(uuid.uuid4())
        self.signaling_port = signaling_port
        self.force_server = force_server
        
        self.discovery = LANPeerDiscovery(self.peer_id, signaling_port)
        self.signaling_server = WebRTCSignalingServer(signaling_port)
        
        self.is_running = False
        
        # If forced server, set discovery state immediately
        if self.force_server:
            self.discovery.is_signaling_server = True
            logger.info(f"Force mode: This node is set as signaling server")
    
    async def start(self):
        """Start the decentralized chat node"""
        self.is_running = True
        logger.info(f"🚀 Starting decentralized chat node: {self.peer_id[:8]}")
        
        # Start peer discovery
        discovery_task = asyncio.create_task(self.discovery.start_discovery())
        
        # Manage signaling server status only if not forced
        if self.force_server:
            # Immediately start signaling server if forced
            signaling_task = asyncio.create_task(self.signaling_server.start_server())
        else:
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
    parser.add_argument('--info-interval', type=int, default=10,
                       help='Interval to print peer info (default: 10 seconds)')
    parser.add_argument('--priority', type=int, default=None,
                       help='Election priority (higher = more likely to be server)')
    parser.add_argument('--force-server', action='store_true', help='Force this node to be signaling server')
    
    args = parser.parse_args()
    
    node = DecentralizedChatNode()
    
    # Set custom priority if specified
    if args.priority is not None:
        node.discovery.priority = args.priority
        logger.info(f"Set custom priority: {args.priority}")
    
    # Start node in background
    node_task = asyncio.create_task(node.start())
    
    # Periodically print peer information
    try:
        while True:
            await asyncio.sleep(args.info_interval)
            info = node.get_peer_info()
            
            print("\n" + "="*80)
            print(f"🤖 Node: {info['self']['peer_id'][:8]} | IP: {info['self']['ip_address']} | Priority: {info['self']['priority']}")
            
            # Signaling server info
            server_info = info['signaling_server']
            if server_info['peer_id']:
                server_status = "👑 THIS NODE" if info['self']['is_signaling_server'] else f"📡 {server_info['peer_id'][:8]}"
                print(f"🏢 Signaling Server: {server_status} | URL: {server_info['websocket_url']}")
            else:
                print("🏢 Signaling Server: ❌ No server elected")
            
            # Network stats
            stats = info['network_stats']
            print(f"🌐 Network: {stats['total_peers']} total peers | Discovery: {stats['discovery_port']} | Signaling: {stats['signaling_port']}")
            
            # Peer list
            if info['peers']:
                print(f"\n📋 Discovered Peers ({len(info['peers'])}):")
                for peer_id, peer_data in info['peers'].items():
                    age = int(time.time() - peer_data['last_seen'])
                    print(f"   🔸 {peer_id[:8]} | {peer_data['ip_address']} | Priority: {peer_data['priority']} | Last seen: {age}s ago")
            else:
                print("\n📋 No other peers discovered")
                
    except KeyboardInterrupt:
        logger.info("🛑 Shutdown requested...")
        node.stop()
        node_task.cancel()
        try:
            await node_task
        except asyncio.CancelledError:
            pass
        logger.info("✅ Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())