import requests
import logging
from typing import List, Dict, Any
from datetime import datetime

# Get logger without configuring it (let main pipeline handle configuration)
logger = logging.getLogger(__name__)

# API Constants
SPACEX_API_BASE = "https://api.spacexdata.com/v4"
LAUNCHES_ENDPOINT = f"{SPACEX_API_BASE}/launches"
LAUNCHES_QUERY_ENDPOINT = f"{SPACEX_API_BASE}/launches/query"
LATEST_ENDPOINT = f"{SPACEX_API_BASE}/launches/latest"


def fetch_latest_launch() -> Dict[str, Any]:
    """
    Fetch the latest launch from SpaceX API.

    This function is used for change detection to determine if new data
    is available before performing a full incremental fetch.

    Returns:
        dict: Latest launch data from SpaceX API

    Raises:
        requests.RequestException: If API call fails
        ValueError: If response is invalid
    """
    try:
        logger.info("Fetching latest launch for change detection")
        response = requests.get(LATEST_ENDPOINT, timeout=30)
        response.raise_for_status()

        data = response.json()
        logger.info(
            f"Latest launch: {data.get('name', 'Unknown')} ({data.get('id', 'Unknown ID')})")
        return data

    except requests.RequestException as e:
        logger.error(f"Failed to fetch latest launch: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid JSON response from latest launch endpoint: {e}")
        raise


def fetch_all_launches() -> List[Dict[str, Any]]:
    """
    Fetch all launches from SpaceX API.

    This function is called only when change detection indicates new data
    is available, optimizing API usage and processing time.

    Returns:
        List[dict]: All launch data from SpaceX API

    Raises:
        requests.RequestException: If API call fails
        ValueError: If response is invalid
    """
    try:
        logger.info("Fetching all launches for incremental processing")
        response = requests.get(LAUNCHES_ENDPOINT, timeout=60)
        response.raise_for_status()

        data = response.json()
        logger.info(f"Fetched {len(data)} total launches from API")
        return data

    except requests.RequestException as e:
        logger.error(f"Failed to fetch all launches: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid JSON response from launches endpoint: {e}")
        raise


def fetch_launches_after_date(date_threshold: datetime) -> List[Dict[str, Any]]:
    """
    Fetch launches after a specific date using efficient POST query with server-side filtering.

    This function handles pagination to ensure ALL matching launches are retrieved,
    not just the first page of results.

    This is the OPTIMIZED approach that fetches only the latest data from the API,
    significantly reducing bandwidth and processing time by filtering server-side.

    Args:
        date_threshold: Only fetch launches after this date

    Returns:
        List[dict]: Filtered launch data from SpaceX API (all pages)

    Raises:
        requests.RequestException: If API call fails
        ValueError: If response is invalid
    """
    try:
        # Convert datetime to ISO format for MongoDB query
        date_str = date_threshold.isoformat()

        all_launches = []
        page = 1
        page_size = 100  # Reasonable page size for better performance

        logger.info(
            f"Fetching launches after {date_str} using paginated POST queries")

        while True:
            # Use MongoDB-style query operators for server-side filtering with pagination
            query_payload = {
                "query": {
                    "date_utc": {"$gte": date_str}
                },
                "options": {
                    "sort": {"date_utc": 1},  # Sort by date ascending
                    "limit": page_size,
                    "page": page
                }
            }

            # Only log first page and every 10th page to reduce verbosity
            if page == 1 or page % 10 == 0:
                logger.info(f"Fetching page {page} (limit: {page_size})")

            response = requests.post(
                LAUNCHES_QUERY_ENDPOINT,
                json=query_payload,
                headers={"Content-Type": "application/json"},
                timeout=60
            )
            response.raise_for_status()

            data = response.json()

            # Extract docs from paginated response
            page_launches = data.get('docs', [])
            total_docs = data.get('totalDocs', 0)
            has_next_page = data.get('hasNextPage', False)
            current_page = data.get('page', page)
            total_pages = data.get('totalPages', 1)

            # Only log progress for first page and every 10th page
            if page == 1 or page % 10 == 0:
                logger.info(
                    f"Page {current_page}/{total_pages}: {len(page_launches)} launches (total matching: {total_docs})")

            # Add this page's launches to our collection
            all_launches.extend(page_launches)

            # Break if no more pages
            if not has_next_page or len(page_launches) == 0:
                break

            page += 1

            # Safety check to prevent infinite loops
            if page > 50:  # Reasonable upper limit
                logger.warning(
                    f"Reached maximum page limit (50), stopping pagination")
                break

        logger.info(
            f"Pagination complete: fetched {len(all_launches)} total launches after {date_str}")
        return all_launches

    except requests.RequestException as e:
        logger.error(f"Failed to fetch launches after date: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid JSON response from filtered launches: {e}")
        raise
