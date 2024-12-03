import pandas as pd
from typing import Optional, List
from dataclasses import dataclass
from datetime import datetime
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
        
        # Initialize history DataFrames with price as index and timestamps as columns
        self.asks_history = pd.DataFrame()
        self.bids_history = pd.DataFrame()

    def _save_snapshot(self, timestamp: str):
        """Save current state to history"""
        if not self.asks_history.empty:
            if not self.asks.empty:
                self.asks_history = self.asks_history.join(other=self.asks.rename(columns={'size': timestamp}), how='outer')
                self.asks_history.sort_index(inplace=True)
        else:
            self.asks_history = self.asks.rename(columns={'size': timestamp})
        
        if not self.bids_history.empty:
            if not self.bids.empty:
                self.bids_history = self.bids_history.join(other=self.bids.rename(columns={'size': timestamp}), how='outer')
                self.bids_history.sort_index(inplace=True, ascending=False)
        else:
            self.bids_history = self.bids.rename(columns={'size': timestamp})

    def update_from_book(self, message: dict):
        """Update entire order book from a book message
        See https://docs.polymarket.com/#market-channel
        """
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
        """Update order book from a price change message
        See https://docs.polymarket.com/#market-channel
        """
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
        """Update last trade information
        See https://docs.polymarket.com/#market-channel
        """
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
            price = self.asks[self.asks['size'] > 0].index.min()
            return price
        return None

    def get_best_bid(self) -> Optional[tuple[float, float]]:
        """Return the best bid price and size"""
        if not self.bids.empty:
            price = self.bids[self.bids['size'] > 0].index.max()
            return price
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
        if timestamp not in self.asks_history.columns or timestamp not in self.bids_history.columns:
            return None, None
            
        asks = pd.DataFrame({'size': self.asks_history[timestamp]}).dropna()
        bids = pd.DataFrame({'size': self.bids_history[timestamp]}).dropna()
        return asks, bids

    def get_timestamps(self) -> List[str]:
        """Get list of all timestamps in chronological order"""
        return sorted(self.asks_history.columns)

    def clear_history_before(self, timestamp: str):
        """Clear history entries before given timestamp to manage memory"""
        timestamps_to_drop = [ts for ts in self.asks_history.columns if ts < timestamp]
        self.asks_history.drop(columns=timestamps_to_drop, inplace=True)
        self.bids_history.drop(columns=timestamps_to_drop, inplace=True)

    def calculate_vwap_mid_price(self, threshold_pct: float = 0.05, timestamp: str = None) -> float:
        """
        Calculate the mid price using Volume-Weighted Average Price (VWAP) from asks and bids,
        considering only prices within a threshold percentage of the best prices.
        
        Args:
            threshold_pct: Percentage threshold from best price (0.1 = 10%)
            timestamp: Optional specific timestamp to calculate VWAP for.
                    If None, uses current order book.
        
        Returns:
            float: VWAP mid price or None if calculation not possible
        """
        if timestamp:
            # Get the specific timestamp column and convert to DataFrame
            asks = pd.DataFrame({'size': self.asks_history[timestamp]}).dropna()
            bids = pd.DataFrame({'size': self.bids_history[timestamp]}).dropna()
            if asks.empty or bids.empty:
                return None
        else:
            asks = self.asks
            bids = self.bids
            if asks.empty or bids.empty:
                return None

        # Get best prices
        best_ask = asks.index.min()
        best_bid = bids.index.max()

        # Calculate price thresholds
        ask_threshold = best_ask * (1 + threshold_pct)
        bid_threshold = best_bid * (1 - threshold_pct)

        # Filter orders within threshold
        valid_asks = asks[asks.index <= ask_threshold]
        valid_bids = bids[bids.index >= bid_threshold]

        # Calculate VWAP for asks
        if not valid_asks.empty:
            asks_total_value = (valid_asks.index * valid_asks['size']).sum()
            asks_total_volume = valid_asks['size'].sum()
            vwap_asks = asks_total_value / asks_total_volume if asks_total_volume > 0 else None
        else:
            vwap_asks = None

        # Calculate VWAP for bids
        if not valid_bids.empty:
            bids_total_value = (valid_bids.index * valid_bids['size']).sum()
            bids_total_volume = valid_bids['size'].sum()
            vwap_bids = bids_total_value / bids_total_volume if bids_total_volume > 0 else None
        else:
            vwap_bids = None

        # Calculate mid price
        if vwap_asks is not None and vwap_bids is not None:
            return (vwap_asks + vwap_bids) / 2
        elif vwap_asks is not None:
            return vwap_asks
        elif vwap_bids is not None:
            return vwap_bids
        else:
            return None
        
    def save_book(self, filename: str = 'book_history'):
        """Save order book history and trade history to CSV file"""
        filename = f'{filename}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}'
        self.asks_history.to_csv(f'{filename}_asks.csv')
        self.bids_history.to_csv(f'{filename}_bids.csv')
        self.trades.to_csv(f'{filename}_trades.csv')
