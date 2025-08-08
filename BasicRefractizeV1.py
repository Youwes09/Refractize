#!/usr/bin/env python3
import asyncio
import json
import logging
from websockets import serve

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleSignalingServer:
    def __init__(self):
        self.clients = {}
        self.peers = set()

    async def handle_connection(self, websocket, path):
        try:
            async for message in websocket:
                data = json.loads(message)
                await self.process_message(websocket, data)
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            client_id = self.get_client_id(websocket)
            if client_id:
                await self.handle_disconnect(client_id)

    async def process_message(self, websocket, data):
        message_type = data.get('type')
        
        if message_type == 'register':
            client_id = data['clientId']
            self.clients[client_id] = websocket
            self.peers.add(client_id)
            logger.info(f"Client registered: {client_id[:8]}")
            
            # Send peer list to all clients
            await self.broadcast_peer_list()
            
        elif message_type in ['offer', 'answer', 'ice_candidate']:
            target_id = data['targetId']
            if target_id in self.clients:
                await self.clients[target_id].send(json.dumps(data))
                logger.debug(f"Forwarded {message_type} to {target_id[:8]}")

    async def broadcast_peer_list(self):
        peer_list = list(self.peers)
        for client_id, ws in self.clients.items():
            try:
                await ws.send(json.dumps({
                    'type': 'peer_list',
                    'peers': [pid for pid in peer_list if pid != client_id]
                }))
            except:
                pass

    async def handle_disconnect(self, client_id):
        if client_id in self.clients:
            del self.clients[client_id]
        if client_id in self.peers:
            self.peers.remove(client_id)
        logger.info(f"Client disconnected: {client_id[:8]}")
        await self.broadcast_peer_list()

    def get_client_id(self, websocket):
        for client_id, ws in self.clients.items():
            if ws == websocket:
                return client_id
        return None

async def main():
    server = SimpleSignalingServer()
    async with serve(
        server.handle_connection, 
        "0.0.0.0", 
        8001,
        # These help with connection stability:
        ping_interval=20,
        ping_timeout=60,
        close_timeout=1
    ):
        logger.info("Signaling server running on ws://0.0.0.0:8001")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())