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

@dataclass
class LastTrade:
    price: float
    size: float
    side: str
    timestamp: str
    fee_rate_bps: str

class OrderBook:
    def __init__(self):
        # Initialize empty DataFrames for current asks and bids
        self.asks = pd.DataFrame(columns=['price', 'size'])
        self.bids = pd.DataFrame(columns=['price', 'size'])
        self.asks.set_index('price', inplace=True)
        self.bids.set_index('price', inplace=True)
        self.last_trade: Optional[LastTrade] = None
        self.trades = pd.DataFrame(columns=['price', 'size', 'side', 'timestamp'])
        
        # Initialize history dictionaries
        self.asks_history: Dict[str, pd.DataFrame] = {}
        self.bids_history: Dict[str, pd.DataFrame] = {}

    def _save_snapshot(self, timestamp: str):
        """Save current state to history"""
        self.asks_history[timestamp] = self.asks.copy()
        self.bids_history[timestamp] = self.bids.copy()

    def update_from_book(self, message: dict):
        """Update entire order book from a book message"""
        if message['event_type'] != 'book':
            return

        # Convert asks and bids to DataFrames
        new_asks = pd.DataFrame(message['asks'])
        new_bids = pd.DataFrame(message['bids'])
        
        # Convert price and size to float
        new_asks[['price', 'size']] = new_asks[['price', 'size']].astype(float)
        new_bids[['price', 'size']] = new_bids[['price', 'size']].astype(float)
        
        # Set price as index
        new_asks.set_index('price', inplace=True)
        new_bids.set_index('price', inplace=True)
        
        # Update the order book
        self.asks = new_asks
        self.bids = new_bids
        
        # Save snapshot
        self._save_snapshot(message['timestamp'])

    def update_from_price_change(self, message: dict):
        """Update order book from a price change message"""
        if message['event_type'] != 'price_change':
            return

        for change in message['changes']:
            price = float(change['price'])
            size = float(change['size'])
            side = change['side']

            if side == 'SELL':  # Update asks
                if size == 0:
                    self.asks.drop(index=price, errors='ignore', inplace=True)
                else:
                    self.asks.loc[price] = size  # Set new aggregate size
            else:  # side == 'BUY', update bids
                if size == 0:
                    self.bids.drop(index=price, errors='ignore', inplace=True)
                else:
                    self.bids.loc[price] = size  # Set new aggregate size

        # Sort the order books
        self.asks.sort_index(inplace=True)
        self.bids.sort_index(ascending=False, inplace=True)
        
        # Save snapshot
        self._save_snapshot(message['timestamp'])

    def update_last_trade(self, message: dict):
        """Update last trade information"""
        if message['event_type'] != 'last_trade_price':
            return

        self.last_trade = LastTrade(
            price=float(message['price']),
            size=float(message['size']),
            side=message['side'],
            timestamp=message['timestamp'],
            fee_rate_bps=message['fee_rate_bps']
        )
        new_trade = pd.DataFrame([{
            'price': self.last_trade.price,
            'size': self.last_trade.size,
            'side': self.last_trade.side,
            'timestamp': self.last_trade.timestamp
        }])
        self.trades = pd.concat([self.trades, new_trade], ignore_index=True)

    def get_best_ask(self) -> Optional[tuple[float, float]]:
        """Return the best ask price and size"""
        if not self.asks.empty:
            price = self.asks.index[0]
            size = self.asks.iloc[0]['size']
            return (price, size)
        return None

    def get_best_bid(self) -> Optional[tuple[float, float]]:
        """Return the best bid price and size"""
        if not self.bids.empty:
            price = self.bids.index[0]
            size = self.bids.iloc[0]['size']
            return (price, size)
        return None

    def get_spread(self) -> Optional[float]:
        """Return the current spread"""
        best_ask = self.get_best_ask()
        best_bid = self.get_best_bid()
        if best_ask and best_bid:
            return best_ask[0] - best_bid[0]
        return None

    def get_order_book_at_timestamp(self, timestamp: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Get the order book state at a specific timestamp"""
        asks = self.asks_history.get(timestamp)
        bids = self.bids_history.get(timestamp)
        return asks, bids

    def get_timestamps(self) -> List[str]:
        """Get list of all timestamps in chronological order"""
        return sorted(self.asks_history.keys())

    def clear_history_before(self, timestamp: str):
        """Clear history entries before given timestamp to manage memory"""
        for ts in list(self.asks_history.keys()):
            if ts < timestamp:
                del self.asks_history[ts]
                del self.bids_history[ts]

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

    async def connect(self):
        """Connect and subscribe to market updates for given asset IDs"""
        self.websocket = await websockets.connect(self.ws_url)
        self.running = True

        # Send subscription message
        subscribe_message = {
            "type": "Market",
            "assets_ids": list(self.outcomes_id.values())
        }
        await self.websocket.send(json.dumps(subscribe_message))
        
        # Start message handler
        asyncio.create_task(self._message_handler())

    async def _message_handler(self):
        """Handle incoming WebSocket messages"""
        try:
            while self.running and self.websocket:
                message = await self.websocket.recv()
                data_list = ast.literal_eval(message)
                
                # Handle both single messages and lists of messages
                messages = data_list if isinstance(data_list, list) else [data_list]
                
                for data in messages:
                    # Ensure data is for our asset
                    if isinstance(data, dict) and 'asset_id' in data:
                        asset_id = data['asset_id']
                        # Get the outcome for this asset_id
                        outcome = self.id_to_outcome.get(asset_id)
                        if outcome and (callbacks := self.callbacks.get(asset_id)):
                            for callback in callbacks:
                                callback(outcome, data)

        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
        except Exception as e:
            print(f"Error in message handler: {e}")
            print(f"Message that caused error: {message}")

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