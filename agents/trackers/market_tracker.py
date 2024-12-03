from agents.polymarket.polymarket import Polymarket
from agents.utils.objects import SimpleMarket
import json
import websockets
import asyncio
from typing import Callable, Dict, List, TypedDict, Union, Literal
import ast
import pandas as pd
from dataclasses import dataclass
from typing import Optional, List, Dict
import traceback
import time

class MarketChannel:
    def __init__(self, market_obj: SimpleMarket):
        self.ws_url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
        self.websocket = None
        self.callbacks: Dict[str, List[Callable[[str, dict], None]]] = {}
        self.running = False
        self.outcomes = [str(outcome) for outcome in market_obj.outcomes]
        self.outcomes_id = {
            outcome: str(market_obj.clob_token_ids[index]) for index, outcome in enumerate(market_obj.outcomes)
        }
        self.id_to_outcome = {v: k for k, v in self.outcomes_id.items()}
        self.last_ping = 0
        self.ping_interval = 30  # Send ping every 30 seconds
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5  # Start with 5 seconds delay

    async def connect(self):
        """Connect and subscribe to market updates for given asset IDs"""
        try:
            self.websocket = await websockets.connect(self.ws_url)
            self.running = True
            self.last_ping = time.time()

            # Send subscription message
            subscribe_message = {
                "type": "Market",
                "assets_ids": list(self.outcomes_id.values())
            }
            await self.websocket.send(json.dumps(subscribe_message))
            
            # Start message handler and ping task
            asyncio.create_task(self._message_handler())
            asyncio.create_task(self._ping_loop())
            
        except Exception as e:
            print(f"Error during connection: {e}")
            raise

    async def _ping_loop(self):
        """Send periodic ping messages to keep the connection alive"""
        while self.running:
            try:
                if self.websocket and time.time() - self.last_ping >= self.ping_interval:
                    await self.websocket.ping()
                    self.last_ping = time.time()
                await asyncio.sleep(1)  # Check every second
            except Exception as e:
                print(f"Error in ping loop: {e}")
                await asyncio.sleep(1)

    async def _reconnect(self):
        """Attempt to reconnect with exponential backoff"""
        attempts = 0
        current_delay = self.reconnect_delay

        while self.running and attempts < self.max_reconnect_attempts:
            try:
                print(f"Attempting to reconnect... (attempt {attempts + 1}/{self.max_reconnect_attempts})")
                await self.connect()
                print("Successfully reconnected!")
                return True
            except Exception as e:
                attempts += 1
                print(f"Reconnection attempt {attempts} failed: {e}")
                if attempts < self.max_reconnect_attempts:
                    await asyncio.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff
        
        print("Max reconnection attempts reached. Giving up.")
        return False

    async def _message_handler(self):
        """Handle incoming WebSocket messages"""
        while self.running:
            try:
                while self.running and self.websocket:
                    message = await self.websocket.recv()
                    self.last_ping = time.time()  # Reset ping timer on any message
                    
                    try:
                        data_list = ast.literal_eval(message)
                    except Exception as e:
                        print(f"Error parsing message: {e}")
                        print(f"Raw message: {message}")
                        continue
                    
                    # Handle both single messages and lists of messages
                    messages = data_list if isinstance(data_list, list) else [data_list]
                    
                    for data in messages:
                        try:
                            # Ensure data is for our asset
                            if isinstance(data, dict) and 'asset_id' in data:
                                asset_id = data['asset_id']
                                # Get the outcome for this asset_id
                                outcome = self.id_to_outcome.get(asset_id)
                                if outcome and (callbacks := self.callbacks.get(asset_id)):
                                    for callback in callbacks:
                                        try:
                                            callback(outcome, data)
                                        except Exception as e:
                                            print(f"Error in callback for outcome {outcome}:")
                                            print(f"Callback: {callback.__name__ if hasattr(callback, '__name__') else callback}")
                                            print(f"Data: {data}")
                                            traceback.print_exc()
                        except Exception as e:
                            print(f"Error processing message data: {e}")
                            print(f"Data causing error: {data}")
                            traceback.print_exc()

            except websockets.exceptions.ConnectionClosed:
                print("WebSocket connection closed unexpectedly")
                if self.running:
                    success = await self._reconnect()
                    if not success:
                        break
                    
            except Exception as e:
                print(f"Fatal error in message handler: {e}")
                traceback.print_exc()
                if self.running:
                    success = await self._reconnect()
                    if not success:
                        break

        print("Message handler stopped")

    def add_outcome_callback(self, outcome: str, callback: Callable[[str, dict], None]):
        """Add a callback for a specific outcome"""
        asset_id = self.outcomes_id[outcome]
        if asset_id not in self.callbacks:
            self.callbacks[asset_id] = []
        self.callbacks[asset_id].append(callback)

    def remove_outcome_callback(self, outcome: str, callback: Callable[[str, dict], None]):
        """Remove a callback for a specific outcome"""
        asset_id = self.outcomes_id[outcome]
        if asset_id in self.callbacks and callback in self.callbacks[asset_id]:
            self.callbacks[asset_id].remove(callback)
            if not self.callbacks[asset_id]:
                del self.callbacks[asset_id]
                
    async def close(self):
        """Close the WebSocket connection"""
        self.running = False
        if self.websocket:
            await self.websocket.close()