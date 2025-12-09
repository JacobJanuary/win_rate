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
            
            # Check timeout exit (after SL/TS checks)
            timeout_result = self._check_timeout_exit(
                candles, entry_price, signal_type, idx,
                max_profit, max_drawdown
            )
            
            if timeout_result:
                return timeout_result
        
        # Fallback: should not reach here if we have 1440 candles
        # But handle edge case of fewer candles
        return self._check_timeout_exit(
            candles, entry_price, signal_type, len(candles) - 1,
            max_profit, max_drawdown
        )
    
    def _check_timeout_exit(
        self,
        candles: List[Dict],
        entry_price: float,
        signal_type: str,
        current_idx: int,
        max_profit: float,
        max_drawdown: float
    ) -> Dict:
        """
        Check for timeout-based exit with graduated strategy
        
        Timeline:
        - 0-20h (0-1199 min): No timeout action
        - 20-21h (1200-1259 min): Exit at breakeven if profitable
        - 21h (1260 min): Force exit at -1%
        - 22h (1320 min): Force exit at -2%
        - 23h (1380 min): Force exit at -3%
        - 24h (1440 min): Force exit at market
        
        Args:
            candles: List of candles
            entry_price: Entry price
            signal_type: 'LONG' or 'SHORT'
            current_idx: Current candle index
            max_profit: Maximum profit achieved
            max_drawdown: Maximum drawdown
        
        Returns:
            Exit result dict if timeout triggered, None otherwise
        """
        minutes_held = current_idx + 1
        
        # No timeout action before hour 20
        if minutes_held < 1200:
            return None
        
        current_candle = candles[current_idx]
        current_price = float(current_candle['close_price'])
        open_time = int(current_candle['open_time'])
        
        # Calculate current PnL
        if signal_type == 'LONG':
            current_pnl = (current_price - entry_price) / entry_price * 100
        else:
            current_pnl = (entry_price - current_price) / entry_price * 100
        
        # Hour 20-21: Breakeven exit if profitable
        if 1200 <= minutes_held < 1260:
            if current_pnl > 0:
                # Exit at entry price (breakeven)
                logger.debug(f"Breakeven exit at {minutes_held} min, PnL was {current_pnl:.2f}%")
                return {
                    'exit_type': 'TIMEOUT_BREAKEVEN',
                    'exit_price': entry_price,
                    'exit_time': open_time,
                    'pnl_pct': 0.0,
                    'max_profit_pct': max_profit,
                    'max_drawdown_pct': max_drawdown,
                    'hold_duration_minutes': minutes_held
                }
            return None  # Continue if not profitable
        
        # Hour 21: Force exit at -1%
        elif minutes_held == 1260:
            exit_price = entry_price * (1 - 0.01) if signal_type == 'LONG' else entry_price * (1 + 0.01)
            logger.debug(f"21h timeout exit at -1%")
            return {
                'exit_type': 'TIMEOUT_21H',
                'exit_price': exit_price,
                'exit_time': open_time,
                'pnl_pct': -1.0,
                'max_profit_pct': max_profit,
                'max_drawdown_pct': max_drawdown,
                'hold_duration_minutes': minutes_held
            }
        
        # Hour 22: Force exit at -2%
        elif minutes_held == 1320:
            exit_price = entry_price * (1 - 0.02) if signal_type == 'LONG' else entry_price * (1 + 0.02)
            logger.debug(f"22h timeout exit at -2%")
            return {
                'exit_type': 'TIMEOUT_22H',
                'exit_price': exit_price,
                'exit_time': open_time,
                'pnl_pct': -2.0,
                'max_profit_pct': max_profit,
                'max_drawdown_pct': max_drawdown,
                'hold_duration_minutes': minutes_held
            }
        
        # Hour 23: Force exit at -3%
        elif minutes_held == 1380:
            exit_price = entry_price * (1 - 0.03) if signal_type == 'LONG' else entry_price * (1 + 0.03)
            logger.debug(f"23h timeout exit at -3%")
            return {
                'exit_type': 'TIMEOUT_23H',
                'exit_price': exit_price,
                'exit_time': open_time,
                'pnl_pct': -3.0,
                'max_profit_pct': max_profit,
                'max_drawdown_pct': max_drawdown,
                'hold_duration_minutes': minutes_held
            }
        
        # Hour 24: Force exit at market
        elif minutes_held >= 1440:
            logger.debug(f"24h timeout exit at market: {current_pnl:.2f}%")
            return {
                'exit_type': 'TIMEOUT_24H',
                'exit_price': current_price,
                'exit_time': open_time,
                'pnl_pct': current_pnl,
                'max_profit_pct': max_profit,
                'max_drawdown_pct': max_drawdown,
                'hold_duration_minutes': minutes_held
            }
        
        return None  # No timeout action yet
    
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
            
            # Skip if simulation returned None (shouldn't happen, but be safe)
            if result is None:
                logger.warning(f"Simulation returned None for signal {signal.get('signal_id', 'unknown')}")
                continue
            
            results.append({
                'signal_id': signal['signal_id'],
                'sl_pct': sl,
                'ts_activation_pct': ts_act,
                'ts_callback_pct': ts_cb,
                **result
            })
            
            self.simulations_run += 1
        
        return results
