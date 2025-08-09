#!/usr/bin/env python3
import asyncio
import websockets
import json
import logging
from typing import Dict, Set, Optional, List
from dataclasses import dataclass, asdict
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ChatMessage:
    sender_id: str
    content: str
    timestamp: float

class SignalingServer:
    def __init__(self):
        self.clients: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.message_history: List[ChatMessage] = []
        self.max_history = 100  # Keep last 100 messages

    async def handle_client(self, websocket, path: Optional[str] = None):
        """Handle new WebSocket connection with full client compatibility"""
        client_id = None
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get('type')

                    if message_type == 'register':
                        client_id = data['clientId']
                        self.clients[client_id] = websocket
                        logger.info(f"Client registered: {client_id[:8]}")

                        # Send welcome package
                        await websocket.send(json.dumps({
                            'type': 'welcome',
                            'yourId': client_id,
                            'peers': [pid for pid in self.clients if pid != client_id],
                            'history': [asdict(m) for m in self.message_history[-self.max_history:]]
                        }))

                        # Notify others about new peer
                        await self.broadcast({
                            'type': 'peer_joined',
                            'peerId': client_id
                        }, exclude=[client_id])

                    elif message_type == 'chat':
                        if client_id:
                            msg = ChatMessage(
                                sender_id=client_id,
                                content=data['message'],
                                timestamp=time.time()
                            )
                            self.message_history.append(msg)
                            if len(self.message_history) > self.max_history:
                                self.message_history.pop(0)
                            
                            await self.broadcast({
                                'type': 'chat',
                                'senderId': client_id,
                                'message': data['message'],
                                'timestamp': msg.timestamp
                            }, exclude=[client_id])

                    elif message_type in ['offer', 'answer', 'ice_candidate']:
                        target_id = data['targetId']
                        if target_id in self.clients:
                            await self.clients[target_id].send(json.dumps({
                                'type': message_type,
                                'senderId': client_id,
                                'data': data['data']
                            }))

                except json.JSONDecodeError:
                    logger.error("Invalid JSON received")
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': 'Invalid JSON format'
                    }))
                except KeyError as e:
                    logger.error(f"Missing field: {str(e)}")
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': f'Missing required field: {str(e)}'
                    }))
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {client_id[:8] if client_id else 'unknown'} disconnected")
        finally:
            if client_id and client_id in self.clients:
                del self.clients[client_id]
                await self.broadcast({
                    'type': 'peer_left',
                    'peerId': client_id
                })

    async def broadcast(self, message, exclude: List[str] = []):
        """Send message to all connected clients except those in exclude list"""
        for peer_id, peer_ws in list(self.clients.items()):
            if peer_id not in exclude:
                try:
                    await peer_ws.send(json.dumps(message))
                except:
                    logger.warning(f"Failed to send to {peer_id[:8]}")
                    del self.clients[peer_id]

async def main():
    server = SignalingServer()
    async with websockets.serve(
        server.handle_client,
        "0.0.0.0",
        8001,
        ping_interval=20,
        ping_timeout=10,
        max_size=10 * 1024 * 1024
    ):
        logger.info("🚀 Signaling server running on ws://0.0.0.0:8001")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())