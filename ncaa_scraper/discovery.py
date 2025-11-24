"""Discovery module for extracting game links and identifying duplicates."""

import json
import logging
import time
from typing import Dict, List, Set
from pathlib import Path

from .config import get_config, Division, Gender
from .scrapers import NCAAScraper
from .scrapers.selenium_utils import SeleniumUtils
from .utils import format_date_for_url, generate_ncaa_urls, parse_url_components
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def discover_games(
    target_date: date,
    output_file: str = "discovery/game_links_mapping.json"
) -> Dict:
    """
    Discover all game links from all divisions and genders for a given date.
    Identifies cross-division duplicates.
    
    Args:
        target_date: Date to discover games for
        output_file: Path to save the mapping JSON file
        
    Returns:
        Dictionary mapping game links to their divisions
    """
    config = get_config()
    scraper = NCAAScraper(config)
    
    # Generate all URLs for the date
    date_str = format_date_for_url(target_date)
    all_divisions = [Division.D1, Division.D2, Division.D3]
    all_genders = [Gender.MEN, Gender.WOMEN]
    
    urls = generate_ncaa_urls(date_str, all_divisions, all_genders)
    logger.info(f"Discovering games for {target_date}: {len(urls)} scoreboard URLs")
    
    # Map to store game_link -> list of (division, gender) tuples
    game_links_map: Dict[str, List[Dict[str, str]]] = {}
    url_to_division_gender: Dict[str, Dict[str, str]] = {}
    
    # Extract game links from each URL
    for url in urls:
        try:
            components = parse_url_components(url)
            division = components['division']
            gender = components['gender']
            
            logger.info(f"Extracting game links from {division} {gender}...")
            
            # Initialize driver
            try:
                scraper.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
            except Exception as e:
                logger.error(f"Failed to initialize WebDriver for {url}: {e}")
                continue
            
            try:
                # Load scoreboard page
                if not scraper._load_scoreboard_page(url, division, gender, f"{components['year']}-{components['month']}-{components['day']}"):
                    logger.warning(f"Failed to load scoreboard page: {url}")
                    continue
                
                # Extract game links
                game_links = scraper._extract_game_links(url)
                
                if not game_links:
                    logger.warning(f"No game links found for {url}")
                    continue
                
                logger.info(f"Found {len(game_links)} game links for {division} {gender}")
                
                # Store game links with their division/gender
                for game_link in game_links:
                    if game_link not in game_links_map:
                        game_links_map[game_link] = []
                    game_links_map[game_link].append({
                        'division': division,
                        'gender': gender
                    })
                
            finally:
                # Cleanup driver
                if scraper.driver:
                    SeleniumUtils.safe_quit_driver(scraper.driver)
                    scraper.driver = None
                    SeleniumUtils._cleanup_driver_resources()
                    time.sleep(1)  # Brief pause between URLs
                    
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            continue
    
    # Identify duplicates and determine primary division
    # Primary division is the first one we encounter (D1 > D2 > D3)
    division_order = {'d1': 1, 'd2': 2, 'd3': 3}
    
    duplicate_mapping = {}
    for game_link, divisions_list in game_links_map.items():
        # Sort divisions by order (D1 first)
        sorted_divisions = sorted(
            divisions_list,
            key=lambda x: division_order.get(x['division'], 999)
        )
        
        primary = sorted_divisions[0]
        is_duplicate = len(sorted_divisions) > 1
        
        duplicate_mapping[game_link] = {
            'primary_division': primary['division'],
            'primary_gender': primary['gender'],
            'divisions': [d['division'] for d in sorted_divisions],
            'genders': [d['gender'] for d in sorted_divisions],
            'is_duplicate': is_duplicate,
            'all_combinations': sorted_divisions
        }
    
    # Count statistics
    total_games = len(duplicate_mapping)
    duplicate_games = sum(1 for v in duplicate_mapping.values() if v['is_duplicate'])
    
    logger.info(f"Discovery complete: {total_games} unique games found")
    logger.info(f"  - {duplicate_games} games appear in multiple divisions")
    logger.info(f"  - {total_games - duplicate_games} games are unique to one division")
    
    # Create final mapping structure
    result = {
        'date': target_date.isoformat(),
        'total_games': total_games,
        'duplicate_games': duplicate_games,
        'game_links': duplicate_mapping
    }
    
    # Save to file
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved game links mapping to {output_path.absolute()}")
    
    return result


def load_game_links_mapping(mapping_file: str) -> Dict:
    """Load game links mapping from JSON file."""
    with open(mapping_file, 'r') as f:
        return json.load(f)


def get_games_for_division_gender(
    mapping: Dict,
    division: str,
    gender: str
) -> List[str]:
    """
    Get list of game links for a specific division and gender.
    
    Args:
        mapping: Game links mapping dictionary
        division: Division (d1, d2, d3)
        gender: Gender (men, women)
        
    Returns:
        List of game links for the specified division and gender
    """
    game_links = []
    
    for game_link, info in mapping.get('game_links', {}).items():
        # Check if this division/gender combination should scrape this game
        for combo in info.get('all_combinations', []):
            if combo['division'] == division and combo['gender'] == gender:
                game_links.append(game_link)
                break
    
    return game_links


def is_duplicate_game(mapping: Dict, game_link: str, division: str) -> bool:
    """
    Check if a game is a duplicate (appears in multiple divisions).
    
    Args:
        mapping: Game links mapping dictionary
        game_link: Game link to check
        division: Current division
        
    Returns:
        True if game is duplicate and current division is not primary
    """
    game_info = mapping.get('game_links', {}).get(game_link)
    if not game_info:
        return False
    
    if not game_info.get('is_duplicate', False):
        return False
    
    # Check if current division is the primary division
    return game_info.get('primary_division') != division

