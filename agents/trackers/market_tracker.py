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
import logging
from pprint import pformat

class MarketChannel:
    def __init__(self, 
                 market_obj: SimpleMarket,
                 logger: logging.Logger = logging.getLogger(__name__)):
        self.logger = logger
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

        self.message_handler_task = None
        self.ping_task = None

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

            # Cancel any existing tasks if we're reconnecting
            if self.message_handler_task is not None and isinstance(self.message_handler_task, asyncio.Task):
                self.logger.debug("Cancelling existing message handler task")
                self.message_handler_task.cancel()
            if self.ping_task is not None and isinstance(self.ping_task, asyncio.Task):
                self.logger.debug("Cancelling existing ping task")
                self.ping_task.cancel()
            
            # Start message handler and ping task
            self.message_handler_task = asyncio.create_task(self._message_handler())
            self.ping_task = asyncio.create_task(self._ping_loop())

            self.logger.info("Connected to market successfully")
            
        except Exception as e:
            self.logger.error(f"Error during connection: {traceback.format_exc()}")
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
                self.logger.error(f"Error in ping loop: {traceback.format_exc()}")
                await asyncio.sleep(1)

    async def _reconnect(self):
        """Attempt to reconnect with exponential backoff"""
        attempts = 0
        current_delay = self.reconnect_delay

        while self.running and attempts < self.max_reconnect_attempts:
            try:
                self.logger.info(f"Attempting to reconnect... (attempt {attempts + 1}/{self.max_reconnect_attempts})")
                await self.connect()
                self.logger.info("Successfully reconnected!")
                return True
            except Exception as e:
                attempts += 1
                self.logger.warning(f"Reconnection attempt {attempts} failed: {traceback.format_exc()}")
                if attempts < self.max_reconnect_attempts:
                    await asyncio.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff
        
        self.logger.error("Max reconnection attempts reached. Giving up.")
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
                        self.logger.warning(f"Error parsing message: {traceback.format_exc()}")
                        self.logger.warning(f"Raw message: {message}")
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
                                            self.logger.error(f"Error in callback for outcome {outcome}:")
                                            self.logger.error(f"Callback: {callback.__name__ if hasattr(callback, '__name__') else callback}")
                                            self.logger.error(f"Data: {data}")
                                            self.logger.error(traceback.format_exc())
                                        else:
                                            self.logger.debug(f"Callback for data {pformat(data)} executed successfully")
                        except Exception as e:
                            self.logger.error("Error processing message data")
                            self.logger.error(f"Data causing error: {pformat(data)}")
                            self.logger.error(traceback.format_exc())

            except websockets.exceptions.ConnectionClosed:
                if self.running:
                    self.logger.warning("WebSocket connection closed unexpectedly")
                    success = await self._reconnect()
                    if not success:
                        break
                    
            except Exception as e:
                self.logger.error("Fatal error in message handler")
                self.logger.error(traceback.format_exc())
                if self.running:
                    success = await self._reconnect()
                    if not success:
                        break

        self.logger.info("Message handler stopped")

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
            self.logger.info("Closing WebSocket connection")
            await self.websocket.close()
            self.logger.info("WebSocket connection closed")