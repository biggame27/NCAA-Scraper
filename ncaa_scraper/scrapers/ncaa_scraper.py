"""NCAA-specific scraper implementation."""

import time
import logging
from typing import List, Optional, Set, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup
import re

from .base_scraper import BaseScraper
from .selenium_utils import SeleniumUtils
from ..models import GameData, TeamData
from ..utils import parse_url_components, extract_game_id_from_url
from ..config.constants import ErrorType

logger = logging.getLogger(__name__)


class NCAAScraper(BaseScraper):
    """NCAA basketball box score scraper."""
    
    def __init__(self, config):
        super().__init__(config)
        self.driver: Optional[webdriver.Chrome] = None
    
    def scrape(self, url: str) -> List[GameData]:
        """
        Scrape NCAA box scores from a scoreboard URL.
        
        Args:
            url: NCAA scoreboard URL
        
        Returns:
            List of scraped game data
        """
        try:
            # Parse URL components
            components = parse_url_components(url)
            year = components['year']
            month = components['month']
            day = components['day']
            gender = components['gender']
            division = components['division']
            
            # Create CSV path
            csv_path = self.file_manager.get_csv_path(year, month, day, gender, division)
            
            # Check if data already exists locally
            # Skip this check if force_rescrape is enabled
            if not getattr(self, 'force_rescrape', False) and self.file_manager.file_exists_and_has_content(csv_path):
                self.logger.info(f"Data for {gender} {division} on {year}-{month}-{day} already exists locally, skipping...")
                return []
            
            # Check if data already exists in Google Drive (if enabled)
            # Skip this check if force_rescrape is enabled
            if self.config.upload_to_gdrive and not getattr(self, 'force_rescrape', False):
                gdrive_exists, gdrive_file_id = self.google_drive.check_file_exists_in_gdrive(
                    year, month, gender, division, day
                )
                if gdrive_exists:
                    self.logger.info(f"Data for {gender} {division} on {year}-{month}-{day} already exists in Google Drive, skipping...")
                    return []
            
            self.logger.info(f"Processing: {csv_path}")
            
            # Initialize driver with retry mechanism
            try:
                self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
            except Exception as e:
                error_msg = f"Failed to initialize WebDriver: {e}"
                self.logger.error(error_msg)
                self.send_notification(
                    error_msg,
                    ErrorType.ERROR,
                    division=division,
                    date=f"{year}-{month}-{day}",
                    gender=gender
                )
                return []
            
            try:
                # Load scoreboard page
                if not self._load_scoreboard_page(url, division, gender, f"{year}-{month}-{day}"):
                    return []
                
                # Get game links (pass URL for retry purposes)
                game_links = self._extract_game_links(url)
                if not game_links:
                    no_links_msg = f"No valid game links found for {url}"
                    self.logger.warning(no_links_msg)
                    self.send_notification(
                        no_links_msg,
                        ErrorType.WARNING,
                        division=division,
                        date=f"{year}-{month}-{day}",
                        gender=gender
                    )
                    return []
                
                # Filter links - keep cross-division duplicates, skip same-division duplicates
                new_links = []
                cross_division_duplicates = []
                for link in game_links:
                    if link in self.visited_links:
                        # Check if it was visited in a different division
                        previous_division = self.visited_links[link]
                        if previous_division != division:
                            # Cross-division duplicate - scrape it but mark it
                            cross_division_duplicates.append(link)
                            new_links.append(link)
                        # If same division, skip it (already visited in this division)
                    else:
                        # Not visited yet
                        new_links.append(link)
                
                skipped_count = len(game_links) - len(new_links)
                
                if skipped_count > 0:
                    self.logger.info(f"Found {len(game_links)} total games, {skipped_count} already visited in same division, {len(new_links)} new games to scrape")
                else:
                    self.logger.info(f"Found {len(game_links)} games to scrape")
                
                if cross_division_duplicates:
                    self.logger.info(f"Found {len(cross_division_duplicates)} cross-division duplicate games that will be marked")
                
                # Scrape each game
                scraped_games = []
                for idx, game_link in enumerate(new_links):
                    try:
                        # Recreate driver every 20 games to prevent memory/resource buildup
                        if idx > 0 and idx % 20 == 0:
                            self.logger.info(f"Recreating driver after {idx} games to prevent resource buildup...")
                            try:
                                # Aggressive cleanup before recreating
                                SeleniumUtils._cleanup_driver_resources()
                                SeleniumUtils.safe_quit_driver(self.driver)
                            except Exception as e:
                                self.logger.warning(f"Error quitting old driver: {e}")
                            
                            # Wait a bit for processes to fully terminate
                            time.sleep(3)
                            
                            # Create new driver
                            try:
                                self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                            except Exception as e:
                                self.logger.error(f"Failed to recreate driver: {e}")
                                # Try once more with longer cleanup
                                SeleniumUtils._cleanup_driver_resources()
                                time.sleep(5)
                                try:
                                    self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                                except Exception as e2:
                                    self.logger.error(f"Failed to recreate driver after cleanup: {e2}")
                                    return scraped_games  # Return what we have so far
                        
                        game_data = self._scrape_single_game(
                            game_link, year, month, day, gender, division, csv_path
                        )
                        if game_data:
                            scraped_games.append(game_data)
                            # Update visited_links with current division
                            self.visited_links[game_link] = division
                    except Exception as e:
                        self.logger.error(f"Error scraping game {game_link}: {e}")
                        self.send_notification(
                            f"Error scraping game: {e}",
                            ErrorType.GAME_ERROR,
                            division=division,
                            date=f"{year}-{month}-{day}",
                            gender=gender,
                            game_link=game_link
                        )
                        continue
                
                # Upload to Google Drive if enabled
                if self.config.upload_to_gdrive and self.file_manager.file_exists_and_has_content(csv_path):
                    self.logger.info(f"Uploading completed CSV for {gender} {division}: {csv_path}")
                    self.upload_to_gdrive(csv_path, year, month, gender, division)
                
                return scraped_games
                
            finally:
                if self.driver:
                    SeleniumUtils.safe_quit_driver(self.driver)
                    self.driver = None
                    
        except Exception as e:
            self.logger.error(f"Unexpected error in scrape method: {e}")
            self.send_notification(
                f"Unexpected error in scrape method: {e}",
                ErrorType.ERROR,
                division=components.get('division') if 'components' in locals() else None,
                date=f"{components.get('year')}-{components.get('month')}-{components.get('day')}" if 'components' in locals() else None,
                gender=components.get('gender') if 'components' in locals() else None
            )
            return []
    
    def _load_scoreboard_page(self, url: str, division: str, gender: str, date: str) -> bool:
        """Load the scoreboard page and check for errors."""
        try:
            self.logger.info(f"Loading scoreboard page: {url}")
            
            # Visit main page first to establish session (for stats.ncaa.org)
            try:
                self.logger.info("Visiting stats.ncaa.org to establish session...")
                SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.get("https://stats.ncaa.org"),
                    timeout=30,
                    operation_name="visit stats.ncaa.org main page"
                )
                time.sleep(3)  # Let cookies/session establish
            except Exception as e:
                self.logger.warning(f"Could not visit main page first: {e}, continuing anyway...")
            
            # Add human-like delay before loading
            SeleniumUtils.human_like_delay(1.0, 2.0)
            
            # Navigate with timeout handling and protection against read timeouts
            try:
                # Use safe_driver_operation to prevent read timeout errors
                load_success = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.get(url),
                    timeout=90,  # 90 seconds max (longer than page_load_timeout of 60s)
                    operation_name=f"load scoreboard page {url}"
                )
                
                if load_success is None:
                    # Page load hung - force stop
                    self.logger.warning(f"Page load hung for scoreboard {url}, attempting recovery...")
                    try:
                        SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.execute_script("window.stop();"),
                            timeout=5,
                            operation_name="stop page load"
                        )
                        time.sleep(1)
                    except Exception:
                        # Driver is stuck - recreate it
                        self.logger.warning("Driver unresponsive, recreating...")
                        try:
                            SeleniumUtils.safe_quit_driver(self.driver)
                            self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                            # Retry the page load once
                            load_success = SeleniumUtils.safe_driver_operation(
                                self.driver,
                                lambda: self.driver.get(url),
                                timeout=90,
                                operation_name=f"retry load scoreboard page {url}"
                            )
                            if load_success is None:
                                self.logger.error(f"Page load still hung after driver recreation for {url}")
                                return False
                            # Give page time to stabilize after recovery
                            time.sleep(3)
                        except Exception as e2:
                            self.logger.error(f"Failed to recreate driver: {e2}")
                            return False
                            
            except TimeoutException:
                self.logger.warning(f"Page load timeout for scoreboard {url}, attempting recovery...")
                try:
                    SeleniumUtils.safe_driver_operation(
                        self.driver,
                        lambda: self.driver.execute_script("window.stop();"),
                        timeout=5,
                        operation_name="stop page load"
                    )
                    time.sleep(1)
                except Exception:
                    # Driver is stuck - recreate it
                    self.logger.warning("Driver unresponsive, recreating...")
                    try:
                        SeleniumUtils.safe_quit_driver(self.driver)
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        # Retry the page load once
                        load_success = SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.get(url),
                            timeout=90,
                            operation_name=f"retry load scoreboard page {url}"
                        )
                        if load_success is None:
                            self.logger.error(f"Failed to load page after driver recreation: {url}")
                            return False
                        # Give page time to stabilize after recovery
                        time.sleep(3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        return False
            except Exception as e:
                # Catch ANY exception during get() - driver might be frozen
                error_str = str(e)
                if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                    self.logger.error(f"Driver frozen/unresponsive during scoreboard load for {url}: {e}")
                    # Recreate driver and retry once
                    try:
                        SeleniumUtils.safe_quit_driver(self.driver)
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        # Retry the page load once
                        load_success = SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.get(url),
                            timeout=90,
                            operation_name=f"retry load scoreboard page {url}"
                        )
                        if load_success is None:
                            self.logger.error(f"Failed to load page after driver recreation: {url}")
                            return False
                        # Give page time to stabilize after recovery
                        time.sleep(3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        return False
                else:
                    # Re-raise other exceptions
                    raise
            
            # Add another delay after page load to ensure stability
            SeleniumUtils.human_like_delay(2.0, 4.0)
            
            # Verify driver is responsive before proceeding with a more robust health check
            try:
                # Try a simple operation to verify driver health
                test_result = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: len(self.driver.find_elements(By.TAG_NAME, "body")),
                    timeout=5,
                    default_return=0,
                    operation_name="driver health check"
                )
                if test_result == 0:
                    self.logger.warning("Driver health check failed (no body elements found), recreating driver...")
                    try:
                        SeleniumUtils.safe_quit_driver(self.driver)
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        # Reload page
                        load_success = SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.get(url),
                            timeout=90,
                            operation_name="reload after driver recreation"
                        )
                        if load_success is None:
                            self.logger.error(f"Failed to reload page after driver recreation: {url}")
                            return False
                        time.sleep(3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver during health check: {e2}")
                        return False
            except Exception as e:
                self.logger.warning(f"Driver health check error: {e}, attempting driver recreation...")
                try:
                    SeleniumUtils.safe_quit_driver(self.driver)
                    self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    load_success = SeleniumUtils.safe_driver_operation(
                        self.driver,
                        lambda: self.driver.get(url),
                        timeout=90,
                        operation_name="reload after driver recreation"
                    )
                    if load_success is None:
                        self.logger.error(f"Failed to reload page after driver recreation: {url}")
                        return False
                    time.sleep(3)
                except Exception as e2:
                    self.logger.error(f"Failed to recreate driver during health check: {e2}")
                    return False
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, self.config.wait_timeout)
            
            # If the page explicitly shows a no-games message, treat as non-error and stop
            try:
                no_games = self.driver.find_elements(By.CLASS_NAME, "no-games-message")
                if no_games:
                    no_games_msg = f"No games found on scoreboard page: {url}"
                    self.logger.info(no_games_msg)
                    self.send_notification(
                        no_games_msg,
                        ErrorType.INFO,
                        division=division,
                        date=date,
                        gender=gender
                    )
                    return False
            except Exception:
                # If this probe fails, continue with normal flow
                pass
            
            try:
                # Wait for body to be present (stats.ncaa.org structure)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(2)  # Additional wait for content to load
                return True
            except TimeoutException:
                # Check if page loaded but just has no games
                try:
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')
                    rows = soup.find_all('div', class_='row')
                    cards = []
                    for row in rows:
                        cards.extend(row.find_all('div', class_='card'))
                    if not cards:
                        no_games_msg = f"No games found on scoreboard page: {url}"
                        self.logger.info(no_games_msg)
                        self.send_notification(
                            no_games_msg,
                            ErrorType.INFO,
                            division=division,
                            date=date,
                            gender=gender
                        )
                        return False
                except Exception:
                    pass
                # Check for specific error pages
                error_msg = SeleniumUtils.check_for_errors(self.driver)
                if error_msg:
                    self.logger.warning(f"{error_msg} for {url}")
                    self.send_notification(
                        f"{error_msg} for {url}",
                        ErrorType.WARNING,
                        division=division,
                        date=date,
                        gender=gender
                    )
                    return False
                
                # Check for HTTP errors
                http_error = SeleniumUtils.check_http_status(self.driver)
                if http_error:
                    self.logger.warning(f"{http_error} for {url}")
                    self.send_notification(
                        f"{http_error} for {url}",
                        ErrorType.ERROR,
                        division=division,
                        date=date,
                        gender=gender
                    )
                    return False
                
                # No games found on scoreboard page - send notification
                no_games_msg = f"No games found on scoreboard page: {url}"
                self.logger.warning(no_games_msg)
                self.send_notification(
                    no_games_msg,
                    ErrorType.INFO,
                    division=division,
                    date=date,
                    gender=gender
                )
                return False
                
        except WebDriverException as e:
            error_msg = f"Selenium WebDriver error loading scoreboard page {url}: {e}"
            self.logger.error(error_msg)
            self.send_notification(
                error_msg,
                ErrorType.ERROR,
                division=division,
                date=date,
                gender=gender
            )
            return False
        except Exception as e:
            error_msg = f"Unexpected error loading scoreboard page {url}: {e}"
            self.logger.error(error_msg)
            self.send_notification(
                error_msg,
                ErrorType.ERROR,
                division=division,
                date=date,
                gender=gender
            )
            return False
    
    def _extract_game_links(self, scoreboard_url: Optional[str] = None) -> List[str]:
        """Extract game links from the scoreboard page using BeautifulSoup."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = 2 * (attempt)  # 2s, 4s delays
                    self.logger.info(f"Retrying game link extraction (attempt {attempt + 1}/{max_retries}) after {delay}s delay")
                    time.sleep(delay)
                    
                    # If retrying, reload the page to ensure fresh state
                    if scoreboard_url:
                        self.logger.info("Reloading scoreboard page before retry...")
                        try:
                            # Visit main page first
                            SeleniumUtils.safe_driver_operation(
                                self.driver,
                                lambda: self.driver.get("https://stats.ncaa.org"),
                                timeout=30,
                                operation_name="visit stats.ncaa.org before retry"
                            )
                            time.sleep(2)
                            SeleniumUtils.safe_driver_operation(
                                self.driver,
                                lambda url=scoreboard_url: self.driver.get(url),
                                timeout=90,
                                operation_name="reload scoreboard page for retry"
                            )
                            time.sleep(3)
                        except Exception as e:
                            self.logger.warning(f"Error reloading page for retry: {e}")
                
                # Wait for page to be ready
                try:
                    wait = WebDriverWait(self.driver, self.config.wait_timeout)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    time.sleep(2)  # Additional wait for content
                except TimeoutException:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Page not ready, retrying...")
                        continue
                    self.logger.warning("Page not ready after all retries")
                    return []
                
                # Get page source and parse with BeautifulSoup
                html = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.page_source,
                    timeout=30,
                    default_return="",
                    operation_name="get page source for parsing"
                )
                
                if not html:
                    if attempt < max_retries - 1:
                        continue
                    return []
                
                # Parse with BeautifulSoup (like altscraper.py)
                soup = BeautifulSoup(html, 'html.parser')
                game_links = []
                seen_contest_ids = set()  # Track contest IDs to avoid duplicates
                
                # Find all rows
                rows = soup.find_all('div', class_='row')
                
                for row in rows:
                    # Find all cards in this row (each card is a game)
                    cards = row.find_all('div', class_='card')
                    
                    for card in cards:
                        try:
                            # Find the table
                            table = card.find('table')
                            if not table:
                                continue
                            
                            # Find box score link
                            box_score_link_elem = table.find('a', href=re.compile(r'/contests/\d+/box_score'))
                            if box_score_link_elem:
                                box_score_path = box_score_link_elem.get('href', '')
                                # Convert to individual_stats URL
                                contest_id_match = re.search(r'/contests/(\d+)/', box_score_path)
                                if contest_id_match:
                                    contest_id = contest_id_match.group(1)
                                    # Only add if we haven't seen this contest ID before
                                    if contest_id not in seen_contest_ids:
                                        seen_contest_ids.add(contest_id)
                                        game_link = f"https://stats.ncaa.org/contests/{contest_id}/individual_stats"
                                        game_links.append(game_link)
                        except Exception as e:
                            self.logger.warning(f"Error parsing game card: {e}")
                            continue
                
                if not game_links:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"No game links found, retrying...")
                        continue
                    self.logger.warning("No game links found after all retries")
                    return []
                
                # Additional deduplication check (in case of any edge cases)
                unique_links = list(dict.fromkeys(game_links))  # Preserves order while removing duplicates
                if len(unique_links) < len(game_links):
                    self.logger.info(f"Removed {len(game_links) - len(unique_links)} duplicate game links")
                
                self.logger.info(f"Found {len(unique_links)} unique game links")
                return unique_links
                    
            except Exception as e:
                error_str = str(e)
                if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                    self.logger.error(f"Driver frozen during link extraction (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        # Recreate driver and retry
                        try:
                            SeleniumUtils._cleanup_driver_resources()
                            SeleniumUtils.safe_quit_driver(self.driver)
                        except Exception as e2:
                            self.logger.warning(f"Error during cleanup: {e2}")
                        
                        time.sleep(5)
                        
                        try:
                            self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                            # Visit main page first, then reload scoreboard
                            if scoreboard_url:
                                SeleniumUtils.safe_driver_operation(
                                    self.driver,
                                    lambda: self.driver.get("https://stats.ncaa.org"),
                                    timeout=30,
                                    operation_name="visit stats.ncaa.org before retry"
                                )
                                time.sleep(2)
                                SeleniumUtils.safe_driver_operation(
                                    self.driver,
                                    lambda url=scoreboard_url: self.driver.get(url),
                                    timeout=90,
                                    operation_name="reload scoreboard page"
                                )
                                time.sleep(2)
                        except Exception as e2:
                            self.logger.error(f"Failed to recreate driver: {e2}")
                            if attempt < max_retries - 1:
                                continue
                    else:
                        self.logger.error(f"Failed to extract game links after all retries: {e}")
                        return []
                else:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Error extracting game links (attempt {attempt + 1}/{max_retries}): {e}")
                        continue
                    else:
                        self.logger.error(f"Failed to extract game links after all retries: {e}")
                        return []
        
        return []
    
    def _scrape_single_game(
        self, 
        game_link: str, 
        year: str, 
        month: str, 
        day: str, 
        gender: str, 
        division: str,
        csv_path: str,
        is_duplicate_from_mapping: bool = False
    ) -> Optional[GameData]:
        """Scrape a single game's individual stats data."""
        game_id = extract_game_id_from_url(game_link)
        
        # Check if game already exists in CSV
        if self.is_duplicate(game_id, csv_path):
            self.logger.info(f"Game {game_id} already exists in {csv_path}, skipping...")
            return None
        
        # Check if already visited in this session
        if game_link in self.visited_links:
            previous_division = self.visited_links[game_link]
            if previous_division == division:
                # Same division - skip
                self.logger.info(f"Game link {game_link} already visited in this session for {division}, skipping...")
                return None
            else:
                # Different division - reuse existing data instead of rescraping
                self.logger.info(f"Game link {game_link} already scraped in {previous_division}, reusing existing data for {division}")
                
                # Get the CSV path for the previous division
                previous_csv_path = self.file_manager.get_csv_path(year, month, day, gender, previous_division)
                
                # Read the game data from the previous division's CSV
                existing_game_data = self.csv_handler.get_game_data_by_link(previous_csv_path, game_link)
                
                if existing_game_data is not None and not existing_game_data.empty:
                    # Update the previous division's CSV to mark it as duplicate
                    if self.csv_handler.update_duplicate_flag(previous_csv_path, game_link, duplicate_value=True):
                        # Upload updated CSV to Google Drive if enabled
                        if self.config.upload_to_gdrive:
                            self.logger.info(f"Uploading updated CSV with duplicate flag to Google Drive: {previous_csv_path}")
                            self.upload_to_gdrive(previous_csv_path, year, month, gender, previous_division)
                    
                    # Copy the data to current division's CSV with duplicate flag set
                    existing_game_data = existing_game_data.copy()
                    
                    # Ensure DUPLICATE_ACROSS_DIVISIONS column exists and set to True
                    if 'DUPLICATE_ACROSS_DIVISIONS' not in existing_game_data.columns:
                        existing_game_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                    else:
                        existing_game_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                    
                    # Append to current division's CSV
                    if self.csv_handler.append_game_data(csv_path, existing_game_data):
                        self.logger.info(f"Successfully copied game data from {previous_division} to {division} CSV")
                        # Update visited_links with current division
                        self.visited_links[game_link] = division
                        return None  # Return None since we didn't create new GameData
                    else:
                        self.logger.error(f"Failed to copy game data to {division} CSV")
                        # Fall through to scrape it anyway
                else:
                    self.logger.warning(f"Could not find existing game data in {previous_division} CSV, will scrape instead")
                    # Fall through to scrape it
        
        self.logger.info(f"Scraping: {game_link}")
        
        try:
            # Navigate to individual stats page with timeout handling
            try:
                load_success = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.get(game_link),
                    timeout=90,
                    operation_name=f"load individual stats page {game_link}"
                )
                
                if load_success is None:
                    self.logger.warning(f"Page load hung for game {game_link}, attempting recovery...")
                    try:
                        SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.execute_script("window.stop();"),
                            timeout=5,
                            operation_name="stop page load"
                        )
                        time.sleep(3)
                    except Exception:
                        self.logger.error(f"Driver unresponsive for {game_link}, skipping...")
                        return None
                else:
                    self.logger.info(f"Successfully navigated to: {game_link}")
                    time.sleep(2)
                    
            except TimeoutException:
                self.logger.warning(f"Page load timeout for game {game_link}, attempting recovery...")
                try:
                    SeleniumUtils.safe_driver_operation(
                        self.driver,
                        lambda: self.driver.execute_script("window.stop();"),
                        timeout=5,
                        operation_name="stop page load"
                    )
                    time.sleep(3)
                except Exception:
                    self.logger.error(f"Driver unresponsive for {game_link}, skipping...")
                    return None
            except Exception as e:
                error_str = str(e)
                if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                    self.logger.error(f"Driver frozen/unresponsive during page load for {game_link}: {e}")
                    try:
                        SeleniumUtils._cleanup_driver_resources()
                        SeleniumUtils.safe_quit_driver(self.driver)
                    except Exception as e2:
                        self.logger.warning(f"Error during cleanup: {e2}")
                    
                    time.sleep(5)
                    
                    try:
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        return None
                    return None
                else:
                    raise
            
            # Wait for stat tables to actually appear in the DOM
            wait = WebDriverWait(self.driver, self.config.wait_timeout)
            try:
                # Wait for at least one stat table to appear
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "table[id*='competitor_'][id*='_year_stat_category_0_data_table']")
                    )
                )
                # Give it a moment for the second table to load
                time.sleep(2)
                self.logger.debug(f"Stat tables found in DOM for {game_link}")
            except TimeoutException:
                self.logger.warning(f"Stat tables not found in DOM for {game_link} after waiting, page may not be loaded properly")
                # Still try to get page source in case tables are there but selector didn't match
            
            # Get page source and parse with BeautifulSoup
            html = SeleniumUtils.safe_driver_operation(
                self.driver,
                lambda: self.driver.page_source,
                timeout=30,
                default_return="",
                operation_name="get page source for parsing"
            )
            
            if not html:
                self.logger.warning(f"Could not get page source for {game_link}")
                return None
            
            # Parse individual stats using BeautifulSoup (like altscraper.py)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find all stat tables (one for each team)
            stat_tables = soup.find_all('table', id=re.compile(r'competitor_\d+_year_stat_category_0_data_table'))
            
            if len(stat_tables) < 2:
                error_msg = f"Could not find both team stat tables for {game_link}"
                self.logger.warning(error_msg)
                self.send_notification(
                    error_msg,
                    ErrorType.GAME_ERROR,
                    division=division,
                    date=f"{year}-{month}-{day}",
                    gender=gender,
                    game_link=game_link
                )
                return None
            
            all_players = []
            
            # Extract team names from card headers
            team_names = []
            for table in stat_tables:
                # Find the parent card element
                card = table.find_parent('div', class_='card')
                if card:
                    # Find the card header
                    card_header = card.find('div', class_='card-header')
                    if card_header:
                        # Find the team name link in the header (look for link with target="TEAM_WIN" or href starting with /teams/)
                        team_link = None
                        # First try to find link with target="TEAM_WIN"
                        team_link = card_header.find('a', {'target': 'TEAM_WIN'})
                        if not team_link:
                            # Fallback: find link with href starting with /teams/
                            all_links = card_header.find_all('a', class_='skipMask')
                            for link in all_links:
                                href = link.get('href', '')
                                if href and href.startswith('/teams/'):
                                    team_link = link
                                    break
                        
                        if team_link:
                            team_name = team_link.get_text(strip=True)
                            team_names.append(team_name)
                        else:
                            # Fallback: try to find any text in the header (excluding "Period Stats")
                            header_text = card_header.get_text(strip=True)
                            # Remove "Period Stats" if present
                            if 'Period Stats' in header_text:
                                header_text = header_text.replace('Period Stats', '').strip()
                            if header_text:
                                team_names.append(header_text)
                            else:
                                team_names.append(None)
                    else:
                        team_names.append(None)
                else:
                    team_names.append(None)
            
            # Ensure we have two team names
            if len(team_names) < 2 or team_names[0] is None or team_names[1] is None:
                self.logger.warning(f"Could not extract team names properly for {game_link}, using fallback")
                team1_name = "Team 1"
                team2_name = "Team 2"
            else:
                team1_name = team_names[0]
                team2_name = team_names[1]
            
            for table_idx, table in enumerate(stat_tables):
                team_name = team1_name if table_idx == 0 else team2_name
                opponent_name = team2_name if table_idx == 0 else team1_name
                
                # Find all player rows
                tbody = table.find('tbody')
                if not tbody:
                    continue
                    
                player_rows = tbody.find_all('tr', id=re.compile(r'game_player_\d+_year_stat_category_0'))
                
                # Skip the last row (usually team totals/team name row)
                if len(player_rows) > 0:
                    player_rows = player_rows[:-1]
                
                for row in player_rows:
                    try:
                        # Skip team totals rows
                        if 'TEAM' in row.get_text() or team_name.strip() in row.get_text():
                            continue
                        
                        cells = row.find_all('td')
                        if len(cells) < 20:
                            continue
                        
                        # Extract player data (same as altscraper.py)
                        player_num = cells[0].get_text(strip=True) if len(cells) > 0 else ''
                        
                        name_elem = cells[1].find('a')
                        player_name = name_elem.get_text(strip=True) if name_elem else cells[1].get_text(strip=True)
                        
                        # Extract player ID from href (e.g., /players/10804376 -> 10804376)
                        player_id = ''
                        if name_elem and name_elem.get('href'):
                            href = name_elem.get('href')
                            # Extract the ID from the href (last part after /players/)
                            if '/players/' in href:
                                player_id = href.split('/players/')[-1].split('/')[0].split('?')[0]
                        
                        position = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                        
                        # Convert minutes from "MM:SS" to decimal
                        min_text = cells[3].get_text(strip=True) if len(cells) > 3 else '0:00'
                        minutes = self._convert_minutes_to_decimal(min_text)
                        
                        fgm = cells[4].get_text(strip=True) if len(cells) > 4 else '0'
                        fga = cells[5].get_text(strip=True) if len(cells) > 5 else '0'
                        fgm_a = f"{fgm}-{fga}"
                        
                        fg3m = cells[6].get_text(strip=True) if len(cells) > 6 else '0'
                        fg3a = cells[7].get_text(strip=True) if len(cells) > 7 else '0'
                        fg3m_a = f"{fg3m}-{fg3a}"
                        
                        ftm = cells[8].get_text(strip=True) if len(cells) > 8 else '0'
                        fta = cells[9].get_text(strip=True) if len(cells) > 9 else '0'
                        ftm_a = f"{ftm}-{fta}"
                        
                        pts = cells[10].get_text(strip=True) if len(cells) > 10 else '0'
                        oreb = cells[11].get_text(strip=True) if len(cells) > 11 else '0'
                        dreb = cells[12].get_text(strip=True) if len(cells) > 12 else '0'
                        reb = cells[13].get_text(strip=True) if len(cells) > 13 else '0'
                        ast = cells[14].get_text(strip=True) if len(cells) > 14 else '0'
                        to = cells[15].get_text(strip=True) if len(cells) > 15 else '0'
                        stl = cells[16].get_text(strip=True) if len(cells) > 16 else '0'
                        blk = cells[17].get_text(strip=True) if len(cells) > 17 else '0'
                        pf = cells[18].get_text(strip=True) if len(cells) > 18 else '0'
                        
                        # Create player record as DataFrame row
                        player_dict = {
                            'NO': player_num,
                            'Name': player_name,
                            'PlayerID': player_id,
                            'POS': position,
                            'MIN': minutes,
                            'FGM-A': fgm_a,
                            '3PM-A': fg3m_a,
                            'FTM-A': ftm_a,
                            'OREB': oreb,
                            'REB': reb,
                            'AST': ast,
                            'ST': stl,
                            'BLK': blk,
                            'TO': to,
                            'PF': pf,
                            'PTS': pts,
                            'Unnamed: 15': '',
                            'TEAM': team_name,
                            'OPP': opponent_name,
                            'GAMEID': game_id,
                            'GAMELINK': game_link,
                        }
                        
                        all_players.append(player_dict)
                        
                    except Exception as e:
                        self.logger.warning(f"Error parsing player row: {e}")
                        continue
            
            if not all_players:
                self.logger.warning(f"No player data found for {game_link}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(all_players)
            
            # Split into two teams
            team1_df = df[df['TEAM'] == team1_name].copy()
            team2_df = df[df['TEAM'] == team2_name].copy()
            
            if team1_df.empty or team2_df.empty:
                self.logger.warning(f"Could not split teams properly for {game_link}")
                return None
            
            # Create TeamData objects
            team_one_data = TeamData(
                team_name=team1_name,
                opponent_name=team2_name,
                game_id=game_id,
                game_link=game_link,
                stats=team1_df[['NO', 'Name', 'PlayerID', 'POS', 'MIN', 'FGM-A', '3PM-A', 'FTM-A', 'OREB', 'REB', 'AST', 'ST', 'BLK', 'TO', 'PF', 'PTS', 'Unnamed: 15']]
            )
            
            team_two_data = TeamData(
                team_name=team2_name,
                opponent_name=team1_name,
                game_id=game_id,
                game_link=game_link,
                stats=team2_df[['NO', 'Name', 'PlayerID', 'POS', 'MIN', 'FGM-A', '3PM-A', 'FTM-A', 'OREB', 'REB', 'AST', 'ST', 'BLK', 'TO', 'PF', 'PTS', 'Unnamed: 15']]
            )
            
            # Check if this is a cross-division duplicate
            # Use mapping flag if provided (for parallel scraping), otherwise use visited_links
            if is_duplicate_from_mapping:
                is_cross_division_duplicate = True
            else:
                is_cross_division_duplicate = (
                    game_link in self.visited_links and 
                    self.visited_links[game_link] != division
                )
            
            # Create game data
            game_data = GameData(
                game_id=game_id,
                game_link=game_link,
                team_one=team_one_data,
                team_two=team_two_data,
                date=f"{year}-{month}-{day}",
                division=division,
                gender=gender,
                duplicate_across_divisions=is_cross_division_duplicate
            )
            
            # Save to CSV
            if self.save_game_data(game_data, csv_path):
                self.logger.info(f"Successfully saved game data for {game_id}")
                return game_data
            else:
                self.logger.error(f"Failed to save game data for {game_id}")
                return None
                
        except Exception as e:
            error_str = str(e)
            if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                self.logger.error(f"Driver frozen/unresponsive during game scrape for {game_link}: {e}")
                try:
                    SeleniumUtils._cleanup_driver_resources()
                    SeleniumUtils.safe_quit_driver(self.driver)
                except Exception as e2:
                    self.logger.warning(f"Error during cleanup: {e2}")
                
                time.sleep(5)
                
                try:
                    self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                except Exception as e2:
                    self.logger.error(f"Failed to recreate driver: {e2}")
                    SeleniumUtils._cleanup_driver_resources()
                    time.sleep(10)
                    try:
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    except Exception as e3:
                        self.logger.error(f"Failed to recreate driver after extended wait: {e3}")
                return None
            else:
                error_msg = f"Error scraping game {game_link}: {e}"
                self.logger.error(error_msg)
                self.send_notification(
                    error_msg,
                    ErrorType.GAME_ERROR,
                    division=division,
                    date=f"{year}-{month}-{day}",
                    gender=gender,
                    game_link=game_link
                )
                return None
    
    def _convert_minutes_to_decimal(self, min_text: str) -> float:
        """Convert minutes from 'MM:SS' format to decimal (e.g., '31:36' -> 31.6)."""
        try:
            if ':' in min_text:
                parts = min_text.split(':')
                minutes = int(parts[0])
                seconds = int(parts[1])
                decimal = round(minutes + (seconds / 60), 1)
                return decimal
            else:
                return float(min_text)
        except:
            return 0.0
    
    def _extract_team_names(self, team_selector) -> List[str]:
        """Extract team names from the team selector."""
        try:
            child_divs = team_selector.find_elements(By.TAG_NAME, "div")
            team_names = [div.text.strip() for div in child_divs if div.text.strip()]
            return team_names
        except Exception as e:
            self.logger.error(f"Error extracting team names: {e}")
            return []
    
    def _extract_team_data(self, team_selector, team_name: str, opponent_name: str, game_id: str, game_link: str) -> Optional[TeamData]:
        """Extract data for a single team - DEPRECATED, using individual_stats now."""
        # This method is kept for compatibility but individual stats parsing is done in _scrape_single_game
        return None
    
    def _switch_to_second_team(self, team_selector, second_team_name: str) -> bool:
        """Switch to the second team's data - DEPRECATED, using individual_stats now."""
        # This method is kept for compatibility but individual stats parsing is done in _scrape_single_game
        return False
