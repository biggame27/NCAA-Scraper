"""Module for tracking and retrying failed games."""

import json
import logging
from pathlib import Path
from typing import Dict, List
from datetime import date

logger = logging.getLogger(__name__)


def save_failed_game(
    failed_games_file: str,
    game_link: str,
    date: date,
    division: str,
    gender: str,
    error_type: str = "timeout",
    error_message: str = ""
):
    """
    Save a failed game to the failed games file.
    
    Args:
        failed_games_file: Path to the failed games JSON file
        game_link: URL of the failed game
        date: Date of the game
        division: Division (d1, d2, d3)
        gender: Gender (men, women)
        error_type: Type of error (timeout, driver_error, etc.)
        error_message: Error message
    """
    output_path = Path(failed_games_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load existing failed games
    failed_games = {}
    if output_path.exists():
        try:
            with open(output_path, 'r') as f:
                failed_games = json.load(f)
        except Exception as e:
            logger.warning(f"Error loading failed games file: {e}, creating new one")
            failed_games = {}
    
    # Initialize structure if needed
    date_key = date.isoformat()
    if date_key not in failed_games:
        failed_games[date_key] = {}
    
    # Add or update failed game entry
    if game_link not in failed_games[date_key]:
        failed_games[date_key][game_link] = []
    
    # Check if this division/gender combination already exists
    existing_entry = None
    for entry in failed_games[date_key][game_link]:
        if entry['division'] == division and entry['gender'] == gender:
            existing_entry = entry
            break
    
    if existing_entry:
        # Update existing entry
        existing_entry['error_type'] = error_type
        existing_entry['error_message'] = error_message
        existing_entry['retry_count'] = existing_entry.get('retry_count', 0)
    else:
        # Add new entry
        failed_games[date_key][game_link].append({
            'division': division,
            'gender': gender,
            'error_type': error_type,
            'error_message': error_message,
            'retry_count': 0
        })
    
    # Save to file
    with open(output_path, 'w') as f:
        json.dump(failed_games, f, indent=2)
    
    logger.debug(f"Saved failed game: {game_link} ({division} {gender})")


def load_failed_games(failed_games_file: str, target_date: date = None) -> Dict:
    """
    Load failed games from file.
    
    Args:
        failed_games_file: Path to the failed games JSON file
        target_date: Optional date to filter by (if None, returns all)
        
    Returns:
        Dictionary of failed games, optionally filtered by date
    """
    output_path = Path(failed_games_file)
    
    if not output_path.exists():
        logger.info(f"No failed games file found at {output_path}")
        return {}
    
    try:
        with open(output_path, 'r') as f:
            failed_games = json.load(f)
        
        if target_date:
            date_key = target_date.isoformat()
            return {date_key: failed_games.get(date_key, {})}
        
        return failed_games
    except Exception as e:
        logger.error(f"Error loading failed games file: {e}")
        return {}


def get_failed_games_for_division_gender(
    failed_games: Dict,
    target_date: date,
    division: str,
    gender: str
) -> List[str]:
    """
    Get list of failed game links for a specific division and gender.
    
    Args:
        failed_games: Failed games dictionary
        target_date: Date to filter by
        division: Division (d1, d2, d3)
        gender: Gender (men, women)
        
    Returns:
        List of failed game links
    """
    date_key = target_date.isoformat()
    game_links = []
    
    date_failed = failed_games.get(date_key, {})
    for game_link, entries in date_failed.items():
        for entry in entries:
            if entry['division'] == division and entry['gender'] == gender:
                game_links.append(game_link)
                break
    
    return game_links


def mark_game_as_retried(
    failed_games_file: str,
    game_link: str,
    target_date: date,
    division: str,
    gender: str,
    success: bool
):
    """
    Mark a failed game as retried and optionally remove it if successful.
    
    Args:
        failed_games_file: Path to the failed games JSON file
        game_link: URL of the game
        target_date: Date of the game
        division: Division (d1, d2, d3)
        gender: Gender (men, women)
        success: Whether the retry was successful
    """
    output_path = Path(failed_games_file)
    
    if not output_path.exists():
        return
    
    try:
        with open(output_path, 'r') as f:
            failed_games = json.load(f)
        
        date_key = target_date.isoformat()
        if date_key not in failed_games:
            return
        
        if game_link not in failed_games[date_key]:
            return
        
        # Update retry count or remove if successful
        entries = failed_games[date_key][game_link]
        for i, entry in enumerate(entries):
            if entry['division'] == division and entry['gender'] == gender:
                if success:
                    # Remove this entry
                    entries.pop(i)
                    # If no more entries for this game, remove the game entirely
                    if not entries:
                        del failed_games[date_key][game_link]
                else:
                    # Increment retry count
                    entry['retry_count'] = entry.get('retry_count', 0) + 1
                break
        
        # If date has no more failed games, remove it
        if date_key in failed_games and not failed_games[date_key]:
            del failed_games[date_key]
        
        # Save updated file
        with open(output_path, 'w') as f:
            json.dump(failed_games, f, indent=2)
            
    except Exception as e:
        logger.error(f"Error updating failed games file: {e}")






