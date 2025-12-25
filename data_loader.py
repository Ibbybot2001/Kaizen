import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, time
import pytz

class DataLoader:
    """
    Responsible for loading, normalizing, and enriching 1-minute market data.
    Enforces immutable raw data and adds deterministic features.
    """
    
    # Session Definitions (NY Time)
    # Note: Sessions crossing midnight need handling (Asia)
    SESSION_ASIA_START = time(18, 0)
    SESSION_LONDON_START = time(3, 0)
    SESSION_NY_START = time(9, 30)
    SESSION_NY_END = time(17, 0)

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.raw_data = None
        self.processed_data = None

    def load_and_process(self) -> pd.DataFrame:
        """Main pipeline execution."""
        print(f"Loading data from {self.filepath}...")
        df = self._load_csv()
        df = self._normalize_time(df)
        df = self._add_session_info(df)
        df = self._add_features(df)
        
        # Enforce immutability concept by returning a copy and not allowing simple edits upstream
        self.processed_data = df.copy() 
        return self.processed_data

    def _load_csv(self) -> pd.DataFrame:
        try:
            # Load with low memory to ensure we catch dtypes correctly, though dataset is small enough
            df = pd.read_csv(self.filepath)
            
            # Basic validation
            required_cols = ['time', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_cols):
                raise ValueError(f"CSV missing required columns. Found: {df.columns}")
            
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV: {e}")

    def _normalize_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert time column to datetime and normalize to US/Eastern (NY Time).
        This is crucial for session alignment.
        """
        # Parse strings to datetime objects, inferring format for speed
        df['time'] = pd.to_datetime(df['time'], utc=True)
        
        # Convert to US/Eastern
        # Note: 'America/New_York' handles DST automatically
        df['time'] = df['time'].dt.tz_convert('America/New_York')
        
        # Sort and set index? No, keeping time as a column is often easier for vectorization, 
        # but sorting is mandatory.
        df = df.sort_values('time').reset_index(drop=True)
        
        return df

    def _add_session_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Assigns session IDs: ASIA, LONDON, NY, OTHER.
        """
        # Vectorized time extraction
        times = df['time'].dt.time
        
        conditions = [
            (times >= self.SESSION_ASIA_START) | (times < self.SESSION_LONDON_START), # Asia (18:00 - 03:00)
            (times >= self.SESSION_LONDON_START) & (times < self.SESSION_NY_START),   # London (03:00 - 09:30)
            (times >= self.SESSION_NY_START) & (times <= self.SESSION_NY_END)         # NY (09:30 - 17:00)
        ]
        
        choices = ['ASIA', 'LONDON', 'NY']
        
        df['session'] = np.select(conditions, choices, default='OTHER')
        
        # Add Trading Day (Useful for daily aggregation)
        # If time is >= 18:00, it belongs to the NEXT day's session logic in Futures
        # For simplicity in this engine, we will group by calendar day for now or just treat stream as continuous
        # But 'prior day VWAP' needs a reset point. Let's define reset at 18:00 NY.
        
        # Logic: If Hour >= 18, TradingDate = Date + 1 Day, else Date
        # This is strictly for grouping daily calculations
        df['trading_date'] = df['time'].apply(
            lambda x: (x + pd.Timedelta(days=1)).date() if x.hour >= 18 else x.date()
        )
        
        return df

    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds technical scaffolding: ATR, VWAP, Rolling High/Low
        """
        
        # ATR (14)
        # ta.atr returns a Series
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        
        # VWAP
        # Default pandas-ta vwap anchors to typical day start. 
        # We want to anchor to our custom 'trading_date' definition (18:00 reset).
        # We can use pandas-ta 'vwap' with an 'offset' if supported, or calculate manually grouping by trading_date.
        # Manual is safer for custom session boundaries.
        
        # Typical Price
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['pv'] = df['tp'] * df['volume']
        
        # Group by trading_date for VWAP reset
        # Cumulative Sums
        daily_groups = df.groupby('trading_date')
        df['cum_pv'] = daily_groups['pv'].cumsum()
        df['cum_vol'] = daily_groups['volume'].cumsum()
        
        df['vwap'] = df['cum_pv'] / df['cum_vol']
        
        # Previous Day Reference Points (PDH, PDL, PDC)
        # Shift daily stats forward by 1 day
        daily_stats = df.groupby('trading_date').agg(
            pdh=('high', 'max'),
            pdl=('low', 'min'),
            pdc=('close', 'last')
        ).shift(1) # We want PREVIOUS day
        
        # Merge back to 1m dataframe
        df = df.merge(daily_stats, left_on='trading_date', right_index=True, how='left')
        
        # Cleanup temp columns
        df.drop(columns=['tp', 'pv', 'cum_pv', 'cum_vol'], inplace=True)
        
        df['atr'] = df['atr'].fillna(0)
        # Rule 6: No Auto-Healing. If VWAP is NaN (start of data), leave it NaN.
        # df['vwap'] = df['vwap'].fillna(method='bfill') 
        
        return df

if __name__ == "__main__":
    # Test run
    loader = DataLoader(r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv")
    df = loader.load_and_process()
    print(df.head())
    print(df.tail())
    print(df['session'].value_counts())
