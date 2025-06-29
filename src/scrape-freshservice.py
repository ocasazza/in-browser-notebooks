#!/usr/bin/env python3
"""
FreshService Ticket Scraper

A robust, parallelized ticket scraper for FreshService with improved error
handling, better date filtering, and configurable parallelization.
"""

import os
import json
import logging
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
import time
import random
from freshpy.core import FreshPy
import dotenv

dotenv.load_dotenv()


def create_directory_structure(
    export_dir: str, year: int, month: int, day: int
) -> Path:
    """Create directory structure for exports."""
    month_name = calendar.month_name[month]
    dir_path = export_dir / str(year) / month_name / f"{day:02d}"
    if not dir_path.exists():
        logging.info(f"Creating directory: {dir_path}")
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def save_ticket(dir_path: Path, ticket: Dict[str, Any]) -> bool:
    """Save ticket data to JSON file."""
    try:
        file_path = dir_path / f"{ticket["ticket"]["id"]}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(ticket, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logging.error(f"Save failed for ticket {ticket["ticket"]["id"]}: {e}")
        return False


def fetch_ticket(client: any, ticket_id: int) -> Optional[Dict[str, any]]:
    """Fetch a single ticket by id"""
    retries = 5
    for i in range(retries):
        try:
            response = client.tickets.get_ticket(
                ticket_id, include=("stats"), conversations=True, activity=True
            )
            if isinstance(response, dict) and response.get("status_code") == 429:
                delay = (5**i) + (random.random() * 5)
                logging.warning(
                    f"Rate limited on ticket {ticket_id}. Retrying in {delay:.2f}s"
                )
                time.sleep(delay)
                continue
            return response
        except Exception as e:
            logging.error(f"Error fetching ticket {ticket_id}: {e}")
            return None
    logging.error(f"Failed to fetch ticket {ticket_id} after {retries} retries.")
    return None


def process_ticket(client: FreshPy, ticket_id: int, export_dir: Path) -> None:
    """Fetch, process, and save a single ticket."""
    logging.debug(f"Processing ticket {ticket_id}")
    response = fetch_ticket(client, ticket_id)
    logging.debug(f"{response}")
    if response and response.get("ticket", None):
        updated_at_str = response["ticket"]["updated_at"].replace("Z", "+00:00")
        updated_at = datetime.fromisoformat(updated_at_str)
        dir_path = create_directory_structure(
            export_dir,
            year=updated_at.year,
            month=updated_at.month,
            day=updated_at.day,
        )
        save_ticket(dir_path, response)


def process_partition(
    partition_id: int,
    ticket_ids: List[int],
    domain: str,
    api_key: str,
    export_dir: Path,
) -> None:
    """Process a partition of ticket IDs."""
    logging.info("Partition %s starting", partition_id)
    client = FreshPy(domain=domain, api_key=api_key)
    # Using map for functional style, even with side effects
    list(map(lambda tid: process_ticket(client, tid, export_dir), ticket_ids))
    logging.info("Partition %s finished", partition_id)


def main():
    """Main function to run the scraper."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    api_key = os.getenv("FRESH_SERVICE_API_KEY")
    domain = os.getenv("FRESH_SERVICE_DOMAIN")
    # setup export dir
    export_dir = Path(os.getenv("EXPORT_DIR")).resolve()
    export_dir.mkdir(parents=True, exist_ok=True)
    logging.info("Export directory created at: %s", export_dir)
    # split requests into partitions
    start_ticket_id = int(os.getenv("START_TICKET_ID"))
    end_ticket_id = int(os.getenv("END_TICKET_ID"))
    number_partitions = int(os.getenv("NUMBER_PARTITIONS"))
    all_ticket_ids = list(range(start_ticket_id, end_ticket_id + 1))
    if not all_ticket_ids:
        logging.warning("No ticket IDs to process.")
        return
    partition_size = len(all_ticket_ids) // number_partitions
    if partition_size == 0:
        partition_size = 1
    partitions = [
        all_ticket_ids[i : i + partition_size]
        for i in range(0, len(all_ticket_ids), partition_size)
    ]
    logging.debug("Scraping tickets from %s to %s", start_ticket_id, end_ticket_id)
    logging.debug("Created %d partitions of size %d", len(partitions), partition_size)
    logging.debug("Using %s partitions", number_partitions)
    logging.debug("Exporting to %s", export_dir)
    # start parallel requests for each partition using worker pool
    with ThreadPoolExecutor(max_workers=number_partitions) as executor:
        futures = {
            executor.submit(
                process_partition,
                i,
                partition,
                domain,
                api_key,
                export_dir,
            ): partition
            for i, partition in enumerate(partitions)
        }
        for future in as_completed(futures):
            partition = futures[future]
            try:
                future.result()
                logging.debug(
                    "Partition for tickets %s-%s completed successfully.",
                    partition[0],
                    partition[-1],
                )
            except Exception as e:
                logging.error(
                    "An error occurred in partition for tickets %s-%s: %s",
                    partition[0],
                    partition[-1],
                    e,
                    exc_info=True,
                )

    logging.info("Scraping finished. Verifying created files...")
    exported_files = list(export_dir.rglob("*.json"))
    if exported_files:
        logging.info("Successfully exported %d tickets.", len(exported_files))
    else:
        logging.error("No files were exported. Please check the logs for errors.")


if __name__ == "__main__":
    main()
