"""
Position Tracker
Manages active positions and filters duplicate signals
"""

from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class PositionTracker:
    """Track active positions and filter duplicate signals"""
    
    def __init__(self):
        """Initialize tracker"""
        self.active_positions: Dict[str, datetime] = {}
        self.position_history: List[Dict] = []
        self.filtered_count = 0
    
    def is_position_active(self, pair_symbol: str, entry_time: datetime) -> bool:
        """
        Check if pair has an active position at given entry time
        
        Args:
            pair_symbol: Trading pair (e.g., 'BTCUSDT')
            entry_time: Proposed entry time for new signal
        
        Returns:
            True if position is active, False otherwise
        """
        if pair_symbol not in self.active_positions:
            return False
        
        # Check if existing position is still active at entry_time
        existing_exit = self.active_positions[pair_symbol]
        
        if entry_time < existing_exit:
            logger.debug(
                f"Position active for {pair_symbol} until {existing_exit}, "
                f"new signal at {entry_time} rejected"
            )
            return True
        
        return False
    
    def add_position(
        self, 
        pair_symbol: str, 
        entry_time: datetime,
        exit_time: Optional[datetime] = None
    ):
        """
        Add a new position to tracking
        
        Args:
            pair_symbol: Trading pair
            entry_time: Position entry time
            exit_time: Actual exit time (if known), or entry + 24h
        """
        if exit_time is None:
            # Default to 24h window
            exit_time = entry_time + timedelta(hours=24)
        
        self.active_positions[pair_symbol] = exit_time
        
        self.position_history.append({
            'pair_symbol': pair_symbol,
            'entry_time': entry_time,
            'exit_time': exit_time
        })
        
        logger.debug(f"Added position: {pair_symbol} {entry_time} -> {exit_time}")
    
    def update_position_exit(
        self,
        pair_symbol: str,
        actual_exit_time: datetime
    ):
        """
        Update position with actual exit time
        
        Args:
            pair_symbol: Trading pair
            actual_exit_time: When position actually closed
        """
        if pair_symbol in self.active_positions:
            old_exit = self.active_positions[pair_symbol]
            self.active_positions[pair_symbol] = actual_exit_time
            
            logger.debug(
                f"Updated {pair_symbol} exit: {old_exit} -> {actual_exit_time}"
            )
    
    def cleanup_expired(self, current_time: datetime):
        """
        Remove positions that have closed
        
        Args:
            current_time: Current simulation time
        """
        expired = [
            pair for pair, exit_time in self.active_positions.items()
            if exit_time <= current_time
        ]
        
        for pair in expired:
            del self.active_positions[pair]
            logger.debug(f"Removed expired position: {pair}")
    
    def get_stats(self) -> Dict:
        """Get tracking statistics"""
        return {
            'active_positions': len(self.active_positions),
            'total_positions': len(self.position_history),
            'filtered_signals': self.filtered_count
        }
