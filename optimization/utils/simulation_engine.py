"""
Trade Simulation Engine
Core logic for simulating trades with SL/TS parameters
"""

import logging
from typing import List, Dict, Tuple
import numpy as np

logger = logging.getLogger(__name__)


class SimulationEngine:
    """Trade simulation engine for testing SL/TS parameters"""
    
    def __init__(self):
        """Initialize simulation engine"""
        self.simulations_run = 0
    
    def simulate_trade(
        self,
        candles: List[Dict],
        signal_type: str,
        sl_pct: float,
        ts_activation_pct: float,
        ts_callback_pct: float
    ) -> Dict:
        """
        Simulate a single trade with given parameters
        
        Args:
            candles: List of 1m candles (should be 1440 for 24h)
            signal_type: 'LONG' or 'SHORT'
            sl_pct: Stop loss percentage
            ts_activation_pct: Trailing stop activation percentage
            ts_callback_pct: Trailing stop callback percentage
        
        Returns:
            Dict with simulation results
        """
        if not candles or len(candles) == 0:
            return self._empty_result()
        
        entry_price = float(candles[0]['open_price'])
        direction = 1 if signal_type == 'LONG' else -1
        
        # Tracking variables
        max_profit = 0.0
        max_drawdown = 0.0
        ts_activated = False
        ts_trigger_price = None
        
        # Process each candle
        for idx, candle in enumerate(candles):
            high = float(candle['high_price'])
            low = float(candle['low_price'])
            close = float(candle['close_price'])
            open_time = int(candle['open_time'])
            
            # Calculate P&L
            if signal_type == 'LONG':
                high_pnl = (high - entry_price) / entry_price * 100
                low_pnl = (low - entry_price) / entry_price * 100
                close_pnl = (close - entry_price) / entry_price * 100
            else:  # SHORT
                high_pnl = (entry_price - low) / entry_price * 100
                low_pnl = (entry_price - high) / entry_price * 100
                close_pnl = (entry_price - close) / entry_price * 100
            
            # Update max metrics
            max_profit = max(max_profit, high_pnl)
            max_drawdown = min(max_drawdown, low_pnl)
            
            # Check SL hit (happens first, intra-candle)
            if low_pnl <= -sl_pct:
                exit_price = entry_price * (1 - sl_pct/100) if signal_type == 'LONG' else entry_price * (1 + sl_pct/100)
                return {
                    'exit_type': 'SL',
                    'exit_price': exit_price,
                    'exit_time': open_time,
                    'pnl_pct': -sl_pct,
                    'max_profit_pct': max_profit,
                    'max_drawdown_pct': max_drawdown,
                    'hold_duration_minutes': idx + 1
                }
            
            # Check TS activation
            if not ts_activated and high_pnl >= ts_activation_pct:
                ts_activated = True
                # Initial TS trigger = activation - callback
                if signal_type == 'LONG':
                    ts_trigger_price = entry_price * (1 + (ts_activation_pct - ts_callback_pct)/100)
                else:  # SHORT
                    ts_trigger_price = entry_price * (1 - (ts_activation_pct - ts_callback_pct)/100)
                
                logger.debug(f"TS activated at {high_pnl:.2f}%, trigger: {ts_trigger_price}")
            
            # Check TS trigger (if activated)
            if ts_activated:
                if signal_type == 'LONG':
                    # TS triggered if price drops to trigger level
                    if low <= ts_trigger_price:
                        pnl = (ts_trigger_price - entry_price) / entry_price * 100
                        return {
                            'exit_type': 'TS',
                            'exit_price': ts_trigger_price,
                            'exit_time': open_time,
                            'pnl_pct': pnl,
                            'max_profit_pct': max_profit,
                            'max_drawdown_pct': max_drawdown,
                            'hold_duration_minutes': idx + 1
                        }
                    
                    # Update TS trigger (trail upwards)
                    new_trigger = high * (1 - ts_callback_pct/100)
                    ts_trigger_price = max(ts_trigger_price, new_trigger)
                    
                else:  # SHORT
                    # TS triggered if price rises to trigger level
                    if high >= ts_trigger_price:
                        pnl = (entry_price - ts_trigger_price) / entry_price * 100
                        return {
                            'exit_type': 'TS',
                            'exit_price': ts_trigger_price,
                            'exit_time': open_time,
                            'pnl_pct': pnl,
                            'max_profit_pct': max_profit,
                            'max_drawdown_pct': max_drawdown,
                            'hold_duration_minutes': idx + 1
                        }
                    
                    # Update TS trigger (trail downwards)
                    new_trigger = low * (1 + ts_callback_pct/100)
                    ts_trigger_price = min(ts_trigger_price, new_trigger)
        
        # 24h time limit reached
        final_candle = candles[-1]
        final_price = float(final_candle['close_price'])
        
        if signal_type == 'LONG':
            final_pnl = (final_price - entry_price) / entry_price * 100
        else:
            final_pnl = (entry_price - final_price) / entry_price * 100
        
        return {
            'exit_type': 'TIME_LIMIT',
            'exit_price': final_price,
            'exit_time': int(final_candle['open_time']),
            'pnl_pct': final_pnl,
            'max_profit_pct': max_profit,
            'max_drawdown_pct': max_drawdown,
            'hold_duration_minutes': len(candles)
        }
    
    def _empty_result(self) -> Dict:
        """Return empty result for invalid simulations"""
        return {
            'exit_type': 'ERROR',
            'exit_price': None,
            'exit_time': None,
            'pnl_pct': 0.0,
            'max_profit_pct': 0.0,
            'max_drawdown_pct': 0.0,
            'hold_duration_minutes': 0
        }
    
    def simulate_batch(
        self,
        signal: Dict,
        candles: List[Dict],
        param_combinations: List[Tuple[float, float, float]]
    ) -> List[Dict]:
        """
        Simulate all parameter combinations for a signal
        
        Args:
            signal: Signal data
            candles: List of candles
            param_combinations: List of (sl, ts_act, ts_cb) tuples
        
        Returns:
            List of simulation results
        """
        results = []
        
        for sl, ts_act, ts_cb in param_combinations:
            result = self.simulate_trade(
                candles=candles,
                signal_type=signal['signal_type'],
                sl_pct=sl,
                ts_activation_pct=ts_act,
                ts_callback_pct=ts_cb
            )
            
            results.append({
                'signal_id': signal['signal_id'],
                'sl_pct': sl,
                'ts_activation_pct': ts_act,
                'ts_callback_pct': ts_cb,
                **result
            })
            
            self.simulations_run += 1
        
        return results
