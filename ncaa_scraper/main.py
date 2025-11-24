"""Main entry point for the refactored NCAA scraper."""

import argparse
import logging
import os
import shutil
from datetime import date
from typing import List

from .config import get_config, Division, Gender
from .scrapers import NCAAScraper
from .scrapers.selenium_utils import SeleniumUtils
from .utils import get_yesterday, format_date_for_url, generate_ncaa_urls
from .models import ScrapingConfig, DateRange
from .config.constants import ErrorType
from .discovery import discover_games, load_game_links_mapping, get_games_for_division_gender
from .failed_games import save_failed_game, load_failed_games, get_failed_games_for_division_gender, mark_game_as_retried
import time
import json

logger = logging.getLogger(__name__)


def main():
    """Main entry point for the NCAA scraper."""
    parser = argparse.ArgumentParser(description='NCAA Box Score Scraper (Refactored)')
    parser.add_argument('--date', type=str, help='Date in YYYY/MM/DD format (default: yesterday)')
    parser.add_argument('--output-dir', type=str, default='scraped_data', help='Output directory for CSV files')
    parser.add_argument('--backfill', action='store_true', help='Run backfill for specific dates')
    parser.add_argument('--upload-gdrive', action='store_true', help='Upload scraped data to Google Drive (default: enabled)')
    parser.add_argument('--no-upload-gdrive', action='store_true', help='Disable Google Drive upload')
    parser.add_argument('--gdrive-folder-id', type=str, help='Google Drive folder ID to upload to (optional)')
    parser.add_argument('--force-rescrape', action='store_true', help='Force rescrape and override existing Google Drive files')
    parser.add_argument('--divisions', nargs='+', choices=['d1', 'd2', 'd3'], default=['d1', 'd2', 'd3'], 
                       help='Divisions to scrape (default: all divisions)')
    parser.add_argument('--genders', nargs='+', choices=['men', 'women'], default=['men', 'women'], 
                       help='Genders to scrape (default: both genders)')
    parser.add_argument('--discover', action='store_true', help='Discovery mode: extract game links and identify duplicates')
    parser.add_argument('--mapping-file', type=str, help='Path to game links mapping JSON file (for single division/gender scraping)')
    parser.add_argument('--single-division', type=str, choices=['d1', 'd2', 'd3'], help='Scrape single division (requires --mapping-file)')
    parser.add_argument('--single-gender', type=str, choices=['men', 'women'], help='Scrape single gender (requires --mapping-file)')
    parser.add_argument('--test-game', type=str, help='Test scraping a single game by URL or contest ID (e.g., https://stats.ncaa.org/contests/6458485/individual_stats or 6458485)')
    parser.add_argument('--test-game-date', type=str, help='Date for test game in YYYY/MM/DD format (required if using --test-game with contest ID only)')
    parser.add_argument('--test-game-division', type=str, choices=['d1', 'd2', 'd3'], default='d1', help='Division for test game (default: d1)')
    parser.add_argument('--test-game-gender', type=str, choices=['men', 'women'], default='men', help='Gender for test game (default: men)')
    parser.add_argument('--retry-failed', action='store_true', help='Retry scraping failed games from previous runs')
    parser.add_argument('--failed-games-file', type=str, default='failed_games.json', help='Path to failed games JSON file')
    
    args = parser.parse_args()
    
    # Get configuration
    config = get_config()
    if not config.validate():
        return 1
    
    # Override config with command line arguments
    if args.output_dir:
        config.output_dir = args.output_dir
    if args.gdrive_folder_id:
        config.google_drive_folder_id = args.gdrive_folder_id
    if args.upload_gdrive:
        config.upload_to_gdrive = True
    if args.no_upload_gdrive:
        config.upload_to_gdrive = False
    
    # Convert division and gender strings to enums
    divisions = [Division(d) for d in args.divisions]
    genders = [Gender(g) for g in args.genders]
    
    # Create output directory
    import os
    os.makedirs(config.output_dir, exist_ok=True)
    logger.info(f"Output directory: {os.path.abspath(config.output_dir)}")
    
    # Handle test game mode
    if args.test_game:
        logger.info(f"Test game mode: testing {args.test_game}")
        try:
            # Parse game URL or contest ID
            game_link = args.test_game
            if not game_link.startswith('http'):
                # It's just a contest ID, construct the URL
                contest_id = game_link
                game_link = f"https://stats.ncaa.org/contests/{contest_id}/individual_stats"
                logger.info(f"Constructed game URL from contest ID: {game_link}")
            
            # Get date, division, and gender
            if args.test_game_date:
                target_date = _parse_date(args.test_game_date)
            else:
                # If no date provided, use today as fallback (for testing purposes)
                logger.warning("Date not provided for test game, using today's date")
                from datetime import date
                target_date = date.today()
            
            year = str(target_date.year)
            month = f"{target_date.month:02d}"
            day = f"{target_date.day:02d}"
            division = args.test_game_division
            gender = args.test_game_gender
            
            # Initialize scraper
            scraper = NCAAScraper(config)
            scraper.force_rescrape = True  # Always force for testing
            
            # Create CSV path for test output
            csv_path = scraper.file_manager.get_csv_path(year, month, day, gender, division)
            logger.info(f"Test output will be saved to: {csv_path}")
            
            # Initialize driver
            try:
                scraper.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
            except Exception as e:
                logger.error(f"Failed to initialize WebDriver: {e}")
                return 1
            
            try:
                # Test scraping the single game
                logger.info(f"Testing game: {game_link}")
                game_data = scraper._scrape_single_game(
                    game_link, year, month, day, gender, division, csv_path
                )
                
                if game_data:
                    logger.info("✓ Game is scrapeable!")
                    logger.info(f"  Game ID: {game_data.game_id}")
                    logger.info(f"  Teams: {game_data.team_one.team_name} vs {game_data.team_two.team_name}")
                    logger.info(f"  Players scraped: {len(game_data.team_one.stats) + len(game_data.team_two.stats)}")
                    logger.info(f"  Data saved to: {csv_path}")
                    return 0
                else:
                    logger.error("✗ Game scraping failed or returned no data")
                    return 1
                    
            finally:
                if scraper.driver:
                    SeleniumUtils.safe_quit_driver(scraper.driver)
                    scraper.driver = None
                    SeleniumUtils._cleanup_driver_resources()
                    
        except Exception as e:
            logger.error(f"Error testing game: {e}", exc_info=True)
            return 1
    
    # Handle discovery mode
    if args.discover:
        target_date = _parse_date(args.date) if args.date else get_yesterday()
        logger.info(f"Discovery mode: extracting game links for {target_date}")
        try:
            mapping = discover_games(target_date, "discovery/game_links_mapping.json")
            logger.info(f"Discovery completed successfully. Found {mapping['total_games']} games.")
            return 0
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            return 1
    
    # Handle retry failed games mode
    if args.retry_failed:
        target_date = _parse_date(args.date) if args.date else get_yesterday()
        logger.info(f"Retry mode: retrying failed games for {target_date}")
        
        try:
            failed_games = load_failed_games(args.failed_games_file, target_date)
            date_key = target_date.isoformat()
            
            if date_key not in failed_games or not failed_games[date_key]:
                logger.info(f"No failed games found for {target_date}")
                return 0
            
            logger.info(f"Found {sum(len(entries) for entries in failed_games[date_key].values())} failed game entries to retry")
            
            # Initialize scraper
            scraper = NCAAScraper(config)
            scraper.force_rescrape = args.force_rescrape
            
            # Retry games for each division/gender combination
            all_divisions = ['d1', 'd2', 'd3']
            all_genders = ['men', 'women']
            
            total_retried = 0
            total_successful = 0
            
            for division in all_divisions:
                for gender in all_genders:
                    game_links = get_failed_games_for_division_gender(failed_games, target_date, division, gender)
                    
                    if not game_links:
                        continue
                    
                    logger.info(f"Retrying {len(game_links)} failed games for {division} {gender}")
                    
                    _scrape_games_from_mapping(
                        scraper,
                        game_links,
                        target_date,
                        division,
                        gender,
                        config.output_dir,
                        failed_games_file=args.failed_games_file,
                        is_retry=True
                    )
                    
                    total_retried += len(game_links)
            
            logger.info(f"Retry completed: {total_retried} games retried")
            return 0
            
        except Exception as e:
            logger.error(f"Error in retry mode: {e}")
            return 1
    
    # Handle single division/gender scraping (requires mapping file)
    if args.single_division and args.single_gender:
        if not args.mapping_file:
            logger.error("--mapping-file is required when using --single-division and --single-gender")
            return 1
        
        target_date = _parse_date(args.date) if args.date else get_yesterday()
        logger.info(f"Single division/gender mode: {args.single_division} {args.single_gender} for {target_date}")
        
        try:
            mapping = load_game_links_mapping(args.mapping_file)
            game_links = get_games_for_division_gender(mapping, args.single_division, args.single_gender)
            logger.info(f"Found {len(game_links)} games to scrape for {args.single_division} {args.single_gender}")
            
            # Initialize scraper
            scraper = NCAAScraper(config)
            scraper.force_rescrape = args.force_rescrape
            
            # Set duplicate mapping on scraper
            scraper.duplicate_mapping = mapping
            
            # Scrape games
            from .utils import parse_url_components
            components = parse_url_components(generate_ncaa_urls(format_date_for_url(target_date), [Division(args.single_division)], [Gender(args.single_gender)])[0])
            
            _scrape_games_from_mapping(
                scraper,
                game_links,
                target_date,
                args.single_division,
                args.single_gender,
                config.output_dir,
                failed_games_file=args.failed_games_file,
                is_retry=False
            )
            
            logger.info("Scraping completed!")
            return 0
        except Exception as e:
            logger.error(f"Error in single division/gender scraping: {e}")
            return 1
    
    # Initialize scraper
    scraper = NCAAScraper(config)
    
    try:
        if args.backfill:
            # Backfill specific dates
            backfill_dates = [
                date(2025, 1, 12),
                date(2025, 2, 15)  # Add your desired date here
            ]
            
            for target_date in backfill_dates:
                logger.info(f"Backfilling data for {target_date}")
                scraping_config = ScrapingConfig.for_backfill(
                    [target_date], divisions, genders, config.output_dir, 
                    config.upload_to_gdrive, config.google_drive_folder_id,
                    force_rescrape=args.force_rescrape
                )
                _run_scraping_session(scraper, scraping_config)
        else:
            # Regular scraping for specified date or yesterday
            target_date = _parse_date(args.date) if args.date else get_yesterday()
            logger.info(f"Scraping data for {target_date}")
            
            scraping_config = ScrapingConfig.for_single_date(
                target_date, divisions, genders, config.output_dir,
                config.upload_to_gdrive, config.google_drive_folder_id,
                force_rescrape=args.force_rescrape
            )
            _run_scraping_session(scraper, scraping_config)
        
        logger.info("Scraping completed!")
        return 0
        
    except Exception as e:
        error_msg = f"Unexpected error in main function: {e}"
        logger.error(error_msg)
        scraper.send_notification(error_msg, ErrorType.ERROR)
        return 1


def _parse_date(date_str: str) -> date:
    """Parse date string to date object."""
    from datetime import datetime
    try:
        return datetime.strptime(date_str, '%Y/%m/%d').date()
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Expected YYYY/MM/DD")
        raise


def _run_scraping_session(scraper: NCAAScraper, scraping_config: ScrapingConfig):
    """Run a scraping session for the given configuration."""
    # Set force_rescrape flag on scraper instance
    scraper.force_rescrape = scraping_config.force_rescrape
    
    # Generate URLs for all dates in range
    all_urls = []
    current_date = scraping_config.date_range.start_date
    end_date = scraping_config.date_range.end_date or scraping_config.date_range.start_date
    
    from datetime import timedelta
    
    while current_date <= end_date:
        date_str = format_date_for_url(current_date)
        urls = generate_ncaa_urls(date_str, scraping_config.divisions, scraping_config.genders)
        all_urls.extend(urls)
        current_date += timedelta(days=1)
    
    # Pre-check Google Drive for existing files (if enabled and not forcing rescrape)
    if scraping_config.upload_to_gdrive and not scraping_config.force_rescrape:
        logger.info("Pre-checking Google Drive for existing files...")
        _precheck_google_drive(scraper, all_urls)
    elif scraping_config.force_rescrape:
        logger.info("Force rescrape enabled - will override existing Google Drive files")
    
    # Scrape each URL with progress logging and cleanup between URLs
    total_urls = len(all_urls)
    logger.info(f"Starting scraping session: {total_urls} URLs to process")
    
    for idx, url in enumerate(all_urls, 1):
        try:
            logger.info(f"Processing URL {idx}/{total_urls}: {url}")
            scraper.scrape(url)
            
            # Cleanup driver between URLs to prevent resource buildup
            if scraper.driver:
                try:
                    SeleniumUtils.safe_quit_driver(scraper.driver)
                    scraper.driver = None
                    SeleniumUtils._cleanup_driver_resources()
                    time.sleep(2)  # Brief pause between URLs for resource cleanup
                except Exception as cleanup_error:
                    logger.warning(f"Error during driver cleanup between URLs: {cleanup_error}")
                    
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            # Ensure driver is cleaned up even on error
            if scraper.driver:
                try:
                    SeleniumUtils.safe_quit_driver(scraper.driver)
                    scraper.driver = None
                    SeleniumUtils._cleanup_driver_resources()
                except Exception:
                    pass
            continue
    
    logger.info(f"Completed scraping session: {total_urls} URLs processed")


def _scrape_games_from_mapping(
    scraper: NCAAScraper,
    game_links: List[str],
    target_date: date,
    division: str,
    gender: str,
    output_dir: str,
    failed_games_file: str = None,
    is_retry: bool = False
):
    """Scrape games from a list of game links (used in single division/gender mode)."""
    from .utils import format_date_for_url
    from .storage import FileManager
    
    date_str = format_date_for_url(target_date)
    year = str(target_date.year)
    month = f"{target_date.month:02d}"
    day = f"{target_date.day:02d}"
    
    csv_path = scraper.file_manager.get_csv_path(year, month, day, gender, division)
    
    # If retrying, download existing CSV from Google Drive and merge
    existing_csv_path = None
    if is_retry and scraper.config.upload_to_gdrive:
        logger.info(f"Retry mode: Checking for existing CSV in Google Drive for {division} {gender}")
        existing_csv_path = csv_path + ".existing"
        
        # Download existing file from Google Drive
        if scraper.google_drive.download_file_from_gdrive(year, month, gender, division, day, existing_csv_path):
            logger.info(f"Downloaded existing CSV from Google Drive: {existing_csv_path}")
        else:
            logger.info("No existing CSV found in Google Drive, will create new file")
            existing_csv_path = None
    
    logger.info(f"Scraping {len(game_links)} games for {division} {gender}")
    
    # Initialize driver
    try:
        scraper.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {e}")
        return
    
    try:
        scraped_count = 0
        failed_count = 0
        
        for idx, game_link in enumerate(game_links, 1):
            try:
                logger.info(f"Scraping game {idx}/{len(game_links)}: {game_link}")
                
                # Check if duplicate from discovery mapping
                mapping = getattr(scraper, 'duplicate_mapping', {})
                game_info = mapping.get('game_links', {}).get(game_link, {})
                is_duplicate = game_info.get('is_duplicate', False)
                primary_division = game_info.get('primary_division', division)
                
                # If it's a duplicate and we're not the primary division, try to copy first
                if is_duplicate and primary_division != division:
                    logger.info(f"Game is duplicate (primary: {primary_division}), attempting to copy from {primary_division} division")
                    primary_csv_path = scraper.file_manager.get_csv_path(year, month, day, gender, primary_division)
                    
                    # Try to read from primary CSV
                    existing_data = scraper.csv_handler.get_game_data_by_link(primary_csv_path, game_link)
                    if existing_data is not None and not existing_data.empty:
                        # Mark as duplicate and copy
                        existing_data = existing_data.copy()
                        if 'DUPLICATE_ACROSS_DIVISIONS' not in existing_data.columns:
                            existing_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                        else:
                            existing_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                        
                        # Append to current division's CSV
                        if scraper.csv_handler.append_game_data(csv_path, existing_data):
                            logger.info(f"Copied duplicate game data from {primary_division}")
                            scraped_count += 1
                            
                            # Mark as successful if retrying
                            if is_retry and failed_games_file:
                                mark_game_as_retried(failed_games_file, game_link, target_date, division, gender, success=True)
                            
                            continue
                        else:
                            logger.warning(f"Failed to copy, will scrape instead")
                    else:
                        logger.info(f"Primary CSV doesn't exist yet, will scrape and mark as duplicate")
                
                # Scrape the game (will be marked as duplicate if is_duplicate and not primary division)
                # Pass the duplicate status to the scraper
                game_data = scraper._scrape_single_game(
                    game_link, year, month, day, gender, division, csv_path,
                    is_duplicate_from_mapping=is_duplicate and primary_division != division
                )
                
                if game_data:
                    scraped_count += 1
                    
                    # Mark as successful if retrying
                    if is_retry and failed_games_file:
                        mark_game_as_retried(failed_games_file, game_link, target_date, division, gender, success=True)
                else:
                    failed_count += 1
                    
                    # Track failed game
                    if failed_games_file:
                        error_type = "timeout"  # Could be more specific based on error
                        save_failed_game(
                            failed_games_file,
                            game_link,
                            target_date,
                            division,
                            gender,
                            error_type=error_type,
                            error_message="Game failed to scrape"
                        )
                    
                    # Mark as failed if retrying
                    if is_retry and failed_games_file:
                        mark_game_as_retried(failed_games_file, game_link, target_date, division, gender, success=False)
                
                # Recreate driver every 20 games
                if idx > 0 and idx % 20 == 0:
                    logger.info(f"Recreating driver after {idx} games...")
                    try:
                        SeleniumUtils._cleanup_driver_resources()
                        SeleniumUtils.safe_quit_driver(scraper.driver)
                        scraper.driver = None
                        time.sleep(3)
                        scraper.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    except Exception as e:
                        logger.warning(f"Error recreating driver: {e}")
                
            except Exception as e:
                logger.error(f"Error scraping game {game_link}: {e}")
                failed_count += 1
                
                # Track failed game
                if failed_games_file:
                    error_type = "exception"
                    save_failed_game(
                        failed_games_file,
                        game_link,
                        target_date,
                        division,
                        gender,
                        error_type=error_type,
                        error_message=str(e)
                    )
                
                # Mark as failed if retrying
                if is_retry and failed_games_file:
                    mark_game_as_retried(failed_games_file, game_link, target_date, division, gender, success=False)
                
                continue
        
        logger.info(f"Scraped {scraped_count}/{len(game_links)} games successfully ({failed_count} failed)")
        
        # If retrying and we downloaded an existing CSV, merge them
        if is_retry and existing_csv_path and os.path.exists(existing_csv_path):
            logger.info(f"Merging existing CSV with new data for {division} {gender}")
            merged_csv_path = csv_path + ".merged"
            
            if scraper.csv_handler.merge_csv_files(existing_csv_path, csv_path, merged_csv_path):
                # Replace the new CSV with the merged one
                shutil.move(merged_csv_path, csv_path)
                logger.info(f"Successfully merged CSV files for {division} {gender}")
                
                # Clean up the downloaded existing CSV
                try:
                    os.remove(existing_csv_path)
                except Exception as e:
                    logger.warning(f"Failed to remove temporary existing CSV: {e}")
            else:
                logger.warning(f"Failed to merge CSV files, uploading new CSV only")
        
        # Upload to Google Drive if enabled
        if scraper.config.upload_to_gdrive and scraper.file_manager.file_exists_and_has_content(csv_path):
            logger.info(f"Uploading CSV to Google Drive: {csv_path}")
            scraper.upload_to_gdrive(csv_path, year, month, gender, division)
            
    finally:
        if scraper.driver:
            SeleniumUtils.safe_quit_driver(scraper.driver)
            scraper.driver = None
            SeleniumUtils._cleanup_driver_resources()


def _precheck_google_drive(scraper: NCAAScraper, urls: List[str]):
    """Pre-check Google Drive for existing files to provide summary."""
    try:
        from .utils import parse_url_components
        
        existing_count = 0
        total_count = len(urls)
        
        for url in urls:
            try:
                components = parse_url_components(url)
                year = components['year']
                month = components['month']
                day = components['day']
                gender = components['gender']
                division = components['division']
                
                gdrive_exists, _ = scraper.google_drive.check_file_exists_in_gdrive(
                    year, month, gender, division, day
                )
                
                if gdrive_exists:
                    existing_count += 1
                    logger.info(f"✓ {gender} {division} {year}-{month}-{day} already exists in Google Drive")
                else:
                    logger.info(f"✗ {gender} {division} {year}-{month}-{day} needs scraping")
                    
            except Exception as e:
                logger.warning(f"Error checking Google Drive for {url}: {e}")
                continue
        
        logger.info(f"Google Drive pre-check complete: {existing_count}/{total_count} files already exist")
        
    except Exception as e:
        logger.error(f"Error during Google Drive pre-check: {e}")


if __name__ == "__main__":
    exit(main())
