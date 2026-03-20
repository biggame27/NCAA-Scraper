"""URL utility functions for the NCAA scraper."""

from typing import List, Dict, Any
from urllib.parse import urlparse, parse_qs, urlencode
from datetime import datetime
import logging

from ..config.constants import NCAA_BASE_URL, Division, Gender

logger = logging.getLogger(__name__)


def generate_ncaa_urls(
    date_str: str,
    divisions: List[Division] = None,
    genders: List[Gender] = None,
    season_division_id: str = None
) -> List[str]:
    """
    Generate stats.ncaa.org scoreboard URLs for given date and parameters.
    
    Args:
        date_str: Date in YYYY/MM/DD format (will be converted to MM/DD/YYYY)
        divisions: List of divisions to scrape
        genders: List of genders to scrape
        season_division_id: Optional NCAA season division ID (e.g., 17763)
    
    Returns:
        List of stats.ncaa.org scoreboard URLs
    """
    if divisions is None:
        divisions = [Division.D3]
    if genders is None:
        genders = [Gender.WOMEN]
    
    # Parse the input date (YYYY/MM/DD) and convert to MM/DD/YYYY
    date_obj = datetime.strptime(date_str, '%Y/%m/%d').date()
    game_date = date_obj.strftime('%m/%d/%Y')
    
    # Map gender to sport code
    gender_to_sport = {
        Gender.WOMEN: 'WBB',
        Gender.MEN: 'MBB'
    }
    
    # Map division to number
    division_to_num = {
        Division.D1: '1',
        Division.D2: '2',
        Division.D3: '3'
    }
    
    urls = []
    for gender in genders:
        for division in divisions:
            params = {
                'utf8': '✓',  # URL encoded as %E2%9C%93
                'sport_code': gender_to_sport[gender],
                'division': division_to_num[division],
                'game_date': game_date,
                'commit': 'Submit'
            }
            if season_division_id:
                # Historical seasons often require season-specific scoreboard endpoints.
                params['season_division_id'] = ''
                params['conference_id'] = '0'
                params['tournament_id'] = ''
                url = f"https://stats.ncaa.org/season_divisions/{season_division_id}/livestream_scoreboards?{urlencode(params)}"
            else:
                url = f"{NCAA_BASE_URL}?{urlencode(params)}"
            urls.append(url)
    
    return urls


def parse_url_components(url: str) -> Dict[str, str]:
    """
    Parse stats.ncaa.org URL to extract components.
    
    Args:
        url: stats.ncaa.org scoreboard URL
    
    Returns:
        Dictionary with parsed components (gender, division, year, month, day)
    """
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        # Extract sport code and convert to gender
        sport_code = params.get('sport_code', [None])[0]
        if sport_code == 'WBB':
            gender = 'women'
        elif sport_code == 'MBB':
            gender = 'men'
        else:
            raise ValueError(f"Unknown sport_code: {sport_code}")
        
        # Extract division and convert to d1/d2/d3 format
        division_num = params.get('division', [None])[0]
        division_map = {'1': 'd1', '2': 'd2', '3': 'd3'}
        division = division_map.get(division_num)
        if not division:
            raise ValueError(f"Unknown division: {division_num}")
        
        # Parse date (MM/DD/YYYY)
        game_date = params.get('game_date', [None])[0]
        if not game_date:
            raise ValueError("game_date parameter not found")
        
        date_obj = datetime.strptime(game_date, '%m/%d/%Y').date()
        
        return {
            'gender': gender,
            'division': division,
            'year': str(date_obj.year),
            'month': f"{date_obj.month:02d}",
            'day': f"{date_obj.day:02d}"
        }
    except Exception as e:
        logger.error(f"Failed to parse URL components from {url}: {e}")
        raise ValueError(f"Invalid URL format: {url}")


def validate_url(url: str) -> bool:
    """Validate if URL is a valid stats.ncaa.org scoreboard URL."""
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False
        
        # Check if it's stats.ncaa.org
        if 'stats.ncaa.org' not in parsed.netloc:
            return False
        
        # Check if it's a livestream_scoreboards URL
        if '/contests/livestream_scoreboards' not in parsed.path:
            return False
        
        # Check for required parameters
        params = parse_qs(parsed.query)
        required = ['sport_code', 'division', 'game_date']
        return all(param in params for param in required)
    except Exception:
        return False


def extract_game_id_from_url(url: str) -> str:
    """Extract contest ID from game URL."""
    # URL format: https://stats.ncaa.org/contests/6458485/individual_stats
    # or: https://stats.ncaa.org/contests/6458485
    parts = url.rstrip('/').split('/')
    if 'contests' in parts:
        contest_index = parts.index('contests')
        if contest_index + 1 < len(parts):
            return parts[contest_index + 1]
    # Fallback: try to extract from end
    return url.split('/')[-1].split('?')[0]
