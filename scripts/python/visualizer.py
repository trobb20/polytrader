from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

@dataclass
class OrderBookFeatures:
    timestamps: np.ndarray
    best_asks: np.ndarray
    best_bids: np.ndarray
    mid_prices: np.ndarray
    spread: np.ndarray
    v_bid_best: np.ndarray
    v_ask_best: np.ndarray
    v_bid_total: np.ndarray
    v_ask_total: np.ndarray
    imbalance_l1: np.ndarray
    imbalance_l3: np.ndarray
    imbalance_l5: np.ndarray
    price_ma_5: np.ndarray
    price_ma_15: np.ndarray
    price_ma_30: np.ndarray
    price_roc: np.ndarray
    volume_roc: np.ndarray
    price_ema_10: np.ndarray
    price_ema_20: np.ndarray
    volume_ema_10: np.ndarray
    bollinger_middle: np.ndarray
    bollinger_upper: np.ndarray
    bollinger_lower: np.ndarray

def load_book_from_csv(f: Path) -> pd.DataFrame:
    # Load the order book data from CSV files
    asks = pd.read_csv(f.parent / f"{f.stem}_asks.csv", index_col=0)
    bids = pd.read_csv(f.parent / f"{f.stem}_bids.csv", index_col=0) 
    trades = pd.read_csv(f.parent / f"{f.stem}_trades.csv")

    # Convert column names to datetime
    new_ask_cols = list(pd.to_datetime(pd.to_numeric(asks.columns), unit='ms'))
    new_bid_cols = list(pd.to_datetime(pd.to_numeric(bids.columns), unit='ms'))

    # Assign new column names
    asks.columns = new_ask_cols
    bids.columns = new_bid_cols

    # Convert timestamp columns from milliseconds to pandas datetime
    trades['timestamp'] = pd.to_datetime(trades['timestamp'], unit='ms')
    
    return {
        'asks': asks,
        'bids': bids, 
        'trades': trades
    }

def develop_features(book: dict[str, pd.DataFrame]) -> OrderBookFeatures:
    best_asks = []
    best_bids = []
    timestamps = []
    v_bid_best = []  # Volume at best bid
    v_ask_best = []  # Volume at best ask
    v_bid_total = []  # Total bid volume
    v_ask_total = []  # Total ask volume
    imbalance_l1 = []  # Level 1 imbalance
    imbalance_l3 = []  # Level 3 imbalance
    imbalance_l5 = []  # Level 5 imbalance

    for col in book['asks'].columns:
        ask_data = book['asks'][col].dropna()
        bid_data = book['bids'][col].dropna()
        
        if not ask_data.empty and not bid_data.empty:
            timestamps.append(col)
            
            # Best prices
            best_ask = ask_data.index.min()
            best_bid = bid_data.index.max()
            best_asks.append(best_ask)
            best_bids.append(best_bid)
            
            # Volumes at best prices
            v_ask_best.append(ask_data.loc[best_ask])
            v_bid_best.append(bid_data.loc[best_bid])
            
            # Total volumes
            v_ask_total.append(ask_data.sum())
            v_bid_total.append(bid_data.sum())
            
            # Imbalance ratios at different levels
            def calc_imbalance(bid_data, ask_data, levels):
                bid_cum = bid_data.head(levels).sum()
                ask_cum = ask_data.head(levels).sum()
                return (bid_cum - ask_cum) / (bid_cum + ask_cum)
            
            imbalance_l1.append(calc_imbalance(bid_data, ask_data, 1))
            imbalance_l3.append(calc_imbalance(bid_data, ask_data, 3))
            imbalance_l5.append(calc_imbalance(bid_data, ask_data, 5))
    
    # Convert to numpy arrays for calculations
    timestamps = np.array(timestamps)
    best_asks = np.array(best_asks)
    best_bids = np.array(best_bids)
    v_bid_best = np.array(v_bid_best)
    v_ask_best = np.array(v_ask_best)
    v_bid_total = np.array(v_bid_total)
    v_ask_total = np.array(v_ask_total)
    
    # Calculate basic features
    mid_prices = (best_bids + best_asks) / 2
    spread = best_asks - best_bids
    
    # Calculate rolling averages of price
    def rolling_average(data, window):
        return pd.Series(data).rolling(window=window, min_periods=1).mean().values
    
    price_ma_5 = rolling_average(mid_prices, 5)
    price_ma_15 = rolling_average(mid_prices, 15)
    price_ma_30 = rolling_average(mid_prices, 30)
    
    # Calculate EMAs
    def calc_ema(data, span):
        return pd.Series(data).ewm(span=span, adjust=False).mean().values
    
    price_ema_10 = calc_ema(mid_prices, 10)
    price_ema_20 = calc_ema(mid_prices, 20)
    volume_ema_10 = calc_ema(v_bid_total + v_ask_total, 10)

    # Calculate rate of change (ROC) for price and volume
    def calc_roc(data):
        return np.diff(data, prepend=data[0]) / data
    
    price_roc = calc_roc(price_ema_10)
    volume_roc = calc_roc(volume_ema_10)
    
    # Calculate Bollinger Bands (20-period, 2 standard deviations)
    def calc_bollinger_bands(data, window=20, num_std=2):
        rolling_mean = pd.Series(data).rolling(window=window).mean()
        rolling_std = pd.Series(data).rolling(window=window).std()
        upper_band = rolling_mean + (rolling_std * num_std)
        lower_band = rolling_mean - (rolling_std * num_std)
        return rolling_mean.values, upper_band.values, lower_band.values

    bollinger_middle, bollinger_upper, bollinger_lower = calc_bollinger_bands(mid_prices)
    
    # Return features as a dataclass instead of dictionary
    return OrderBookFeatures(
        timestamps=timestamps,
        best_asks=best_asks,
        best_bids=best_bids,
        mid_prices=mid_prices,
        spread=spread,
        v_bid_best=v_bid_best,
        v_ask_best=v_ask_best,
        v_bid_total=v_bid_total,
        v_ask_total=v_ask_total,
        imbalance_l1=np.array(imbalance_l1),
        imbalance_l3=np.array(imbalance_l3),
        imbalance_l5=np.array(imbalance_l5),
        price_ma_5=price_ma_5,
        price_ma_15=price_ma_15,
        price_ma_30=price_ma_30,
        price_roc=price_roc,
        volume_roc=volume_roc,
        price_ema_10=price_ema_10,
        price_ema_20=price_ema_20,
        volume_ema_10=volume_ema_10,
        bollinger_middle=bollinger_middle,
        bollinger_upper=bollinger_upper,
        bollinger_lower=bollinger_lower
    )

def plot_trade_price(book: dict[str, pd.DataFrame]):
    features = develop_features(book)
    buys = book['trades'][book['trades']['side'] == 'BUY']
    sells = book['trades'][book['trades']['side'] == 'SELL']

    # Create figure with subplots
    fig, axs = plt.subplots(4, 1, figsize=(15, 20), sharex=True)
    fig.suptitle('Order Book Analysis', fontsize=16)

    # Plot 1: Price and Trades
    axs[0].plot(features.timestamps, features.best_asks, color='red', alpha=0.7, label='Best Ask')
    axs[0].plot(features.timestamps, features.best_bids, color='green', alpha=0.7, label='Best Bid')
    axs[0].plot(features.timestamps, features.mid_prices, color='blue', alpha=0.7, label='Mid Price')
    axs[0].scatter(buys['timestamp'], buys['price'], color='green', marker='^', label='Buy Trade')
    axs[0].scatter(sells['timestamp'], sells['price'], color='red', marker='v', label='Sell Trade')
    axs[0].set_ylabel('Price')
    axs[0].legend()
    axs[0].set_title('Price Action and Trades')
    axs[0].grid(True)

    # Plot 2: Volume Analysis
    ax2_vol = axs[1]
    ax2_spread = ax2_vol.twinx()  # Create twin axis for spread

    # Plot volumes
    vol_bid_line = ax2_vol.plot(features.timestamps, features.v_bid_total, color='green', alpha=0.7, label='Total Bid Volume')[0]
    vol_ask_line = ax2_vol.plot(features.timestamps, features.v_ask_total, color='red', alpha=0.7, label='Total Ask Volume')[0]
    vol_ema_line = ax2_vol.plot(features.timestamps, features.volume_ema_10, color='blue', alpha=0.7, label='Volume EMA-10')[0]
    
    # Plot spread on twin axis
    spread_line = ax2_spread.plot(features.timestamps, features.spread, color='purple', alpha=0.7, label='Spread')[0]

    # Set labels
    ax2_vol.set_ylabel('Volume')
    ax2_spread.set_ylabel('Spread')

    # Add legends
    lines = [vol_bid_line, vol_ask_line, vol_ema_line, spread_line]
    labels = [l.get_label() for l in lines]
    ax2_vol.legend(lines, labels, loc='upper left')

    ax2_vol.set_title('Volume Analysis & Spread')
    ax2_vol.grid(True)

    # Plot 3: Moving Averages
    axs[2].plot(features.timestamps, features.mid_prices, color='black', alpha=0.5, label='Mid Price')
    axs[2].plot(features.timestamps, features.price_ema_10, color='blue', label='EMA-10')
    axs[2].plot(features.timestamps, features.price_ema_20, color='green', label='EMA-20')
    axs[2].plot(features.timestamps, features.price_ma_30, color='red', label='MA-30')
    axs[2].set_ylabel('Price')
    axs[2].legend()
    axs[2].set_title('Moving Averages')
    axs[2].grid(True)

    # Plot 4: Price ROC, Volume ROC, and Bollinger Bands
    ax4_price = axs[3]
    ax4_vol = ax4_price.twinx()  # Create twin axis for volume ROC

    # Plot Price ROC
    price_roc_line = ax4_price.plot(features.timestamps, features.price_roc * 100, 
                                   color='blue', label='Price ROC (%)', alpha=0.7)[0]
    
    # Plot Volume ROC on twin axis
    vol_roc_line = ax4_vol.plot(features.timestamps, features.volume_roc * 100, 
                               color='red', label='Volume ROC (%)', alpha=0.7)[0]
    
    # Add horizontal line at 0
    ax4_price.axhline(y=0, color='black', linestyle='--', alpha=0.3)
    
    # Set labels and title
    ax4_price.set_ylabel('Price ROC (%)', color='blue')
    ax4_vol.set_ylabel('Volume ROC (%)', color='red')
    ax4_price.tick_params(axis='y', labelcolor='blue')
    ax4_vol.tick_params(axis='y', labelcolor='red')
    
    # Add legends
    lines = [price_roc_line, vol_roc_line]
    labels = [l.get_label() for l in lines]
    ax4_price.legend(lines, labels, loc='upper left')
    
    ax4_price.set_title('Rate of Change Analysis')
    ax4_price.grid(True)

    # Format x-axis
    for ax in axs:
        ax.tick_params(axis='x', rotation=45)
    
    # Add Bollinger Bands to the first plot (price action)
    axs[0].plot(features.timestamps, features.bollinger_upper, 
                color='gray', linestyle='--', alpha=0.5, label='Bollinger Upper')
    axs[0].plot(features.timestamps, features.bollinger_lower, 
                color='gray', linestyle='--', alpha=0.5, label='Bollinger Lower')
    axs[0].legend()

    # Adjust layout to prevent overlap
    plt.tight_layout()
    plt.show()

def main():
    book = load_book_from_csv(Path().cwd().parent.parent / 'data/nba-hou-gsw-2024-12-05/Rockets_book_history')
    plot_trade_price(book)


if __name__ == '__main__':
    main()
