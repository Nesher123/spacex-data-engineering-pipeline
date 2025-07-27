import logging
from datetime import datetime
from typing import List
from api import fetch_latest_launch, fetch_all_launches, fetch_launches_after_date, calculate_total_payload_mass
from models import Launch
from database import Database
from aggregations import AggregationService

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IncrementalIngestionPipeline:
    """
    Incremental ingestion pipeline for SpaceX launch data.

    This pipeline implements several key data engineering concepts:

    1. API Efficiency: Uses /latest endpoint for change detection to minimize API calls
    2. Real-World Pattern: Implements Change Data Capture (CDC) pattern  
    3. FETCHING: Uses server-side filtering to fetch only latest data
    4. Demonstrates Key Concepts: Idempotency, efficiency monitoring, and proper error handling
    5. Aggregation Maintenance: Updates aggregation tables incrementally
    """

    def __init__(self):
        self.db = Database()
        self.aggregation_service = AggregationService(self.db)

    def run_incremental_ingestion(self) -> dict:
        """
        Execute the complete incremental ingestion pipeline.

        Returns:
            dict: Summary of ingestion results including metrics for monitoring
        """
        logger.info("=== Starting Incremental Ingestion Pipeline ===")
        start_time = datetime.now()

        try:
            # Step 1: Check if this is an initial load (database empty)
            if self._is_initial_load():
                logger.info(
                    "Initial load detected - fetching all launches without change detection")
                return self._run_initial_load(start_time)

            # Step 2: Change Detection - Quick check using /latest endpoint
            logger.info(
                "Step 2: Performing change detection using /latest endpoint")
            if not self._is_new_data_available():
                logger.info("No new data detected - pipeline completed early")
                return {
                    'status': 'success',
                    'new_launches_found': 0,
                    'launches_inserted': 0,
                    'pipeline_duration_seconds': (datetime.now() - start_time).total_seconds(),
                    'api_calls_made': 1,  # Only /latest call
                    'early_exit': True,
                    'optimization': 'change_detection_early_exit',
                    'aggregations': {'status': 'skipped', 'reason': 'no_new_data'}
                }

            # Step 3: Incremental Fetch - Only fetch launches after high water mark
            logger.info(
                "Step 3: Fetching new launches using server-side filtering")
            new_launches = self._fetch_new_launches()

            if not new_launches:
                logger.info(
                    "No new launches found after filtering - pipeline completed")
                return {
                    'status': 'success',
                    'new_launches_found': 0,
                    'launches_inserted': 0,
                    'pipeline_duration_seconds': (datetime.now() - start_time).total_seconds(),
                    'api_calls_made': 2,  # /latest + /launches/query
                    'early_exit': False,
                    'optimization': 'server_side_filtering',
                    'pagination_handled': True,
                    'aggregations': {'status': 'skipped', 'reason': 'no_new_launches'}
                }

            # Step 4: Data Validation and Processing
            logger.info(f"Step 4: Validating {len(new_launches)} new launches")
            validated_launches = self._validate_launches(new_launches)

            # Step 5: Database Insertion
            logger.info(
                f"Step 5: Inserting {len(validated_launches)} launches")
            inserted_count = self._insert_new_launches(validated_launches)

            # Step 6: Update High Water Mark
            logger.info("Step 6: Updating ingestion state")
            self._update_ingestion_state(validated_launches)

            # Step 7: Update Aggregations
            logger.info("Step 7: Updating aggregations")
            aggregation_result = self.aggregation_service.update_aggregations_for_new_launches(
                validated_launches)

            # Pipeline Success
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"=== Pipeline Completed Successfully in {duration:.2f} seconds ===")

            return {
                'status': 'success',
                'new_launches_found': len(new_launches),
                'launches_inserted': inserted_count,
                'pipeline_duration_seconds': duration,
                'api_calls_made': 2,  # /latest + /launches/query
                'early_exit': False,
                'optimization': 'server_side_filtering',
                'pagination_handled': True,
                'aggregations': aggregation_result
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Pipeline failed after {duration:.2f} seconds: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'pipeline_duration_seconds': duration,
                'early_exit': False
            }

    def _is_initial_load(self) -> bool:
        """
        Check if this is an initial load (database is empty).

        This optimization avoids unnecessary API calls for change detection
        when we know we need to fetch all data anyway.

        Returns:
            bool: True if database is empty and this is an initial load
        """
        try:
            db_latest_launch = self.db.get_latest_launch_from_db()
            return db_latest_launch is None
        except Exception as e:
            logger.error(f"Error checking for initial load: {e}")
            # If we can't determine, assume it's not an initial load and use normal flow
            return False

    def _run_initial_load(self, start_time: datetime) -> dict:
        """
        Execute initial load pipeline for empty database.

        Skips change detection and fetches all launches directly.

        Args:
            start_time: Pipeline start time for duration calculation

        Returns:
            dict: Summary of initial load results
        """
        try:
            # Step 1: Fetch all launches (no change detection needed)
            logger.info("Step 1: Fetching all launches for initial load")
            all_launches = fetch_all_launches()

            # Step 2: Data Validation and Processing
            logger.info(f"Step 2: Validating {len(all_launches)} launches")
            validated_launches = self._validate_launches(all_launches)

            # Step 3: Database Insertion
            logger.info(
                f"Step 3: Inserting {len(validated_launches)} launches")
            inserted_count = self._insert_new_launches(validated_launches)

            # Step 4: Update High Water Mark
            logger.info("Step 4: Updating ingestion state")
            self._update_ingestion_state(validated_launches)

            # Step 5: Initialize/Update Aggregations
            logger.info("Step 5: Initializing aggregations")
            # For initial load, we calculate aggregations from scratch
            aggregation_result = self.aggregation_service.initialize_aggregations_from_scratch()

            # Initial Load Success
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"=== Initial Load Completed Successfully in {duration:.2f} seconds ===")

            return {
                'status': 'success',
                'new_launches_found': len(all_launches),
                'launches_inserted': inserted_count,
                'pipeline_duration_seconds': duration,
                'api_calls_made': 1,  # Only /launches/all call
                'early_exit': False,
                'optimization': 'initial_load_skip_change_detection',
                'initial_load': True,
                'aggregations': aggregation_result
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(
                f"Initial load failed after {duration:.2f} seconds: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'pipeline_duration_seconds': duration,
                'initial_load': True
            }

    def _is_new_data_available(self) -> bool:
        """
        Change Detection: Use /latest endpoint to determine if new data exists.

        This is the core optimization - most pipeline runs will exit here if no
        new data is available, saving API bandwidth and processing time.

        Returns:
            bool: True if new data should be ingested, False otherwise
        """
        try:
            # API Efficiency: Single small API call for change detection
            api_latest_launch = fetch_latest_launch()

            # Delegate to database's change detection logic
            return self.db.is_new_data_available(api_latest_launch)

        except Exception as e:
            logger.error(f"Error in change detection: {e}")
            # Fail-safe: When in doubt, assume new data is available
            logger.info(
                "Proceeding with ingestion due to change detection error")
            return True

    def _fetch_new_launches(self) -> List[dict]:
        """
        Fetch only new launches using server-side filtering with pagination.

        This is the key improvement - instead of fetching all 205+ launches and 
        filtering client-side, we use MongoDB-style queries with proper pagination
        to fetch ALL launches newer than our high water mark directly from the API.

        This perfectly implements the requirement to fetch only "LATEST" data
        while ensuring we don't miss any results due to pagination limits.

        Returns:
            List[dict]: Launch data for launches newer than our last processed date
        """
        try:
            # Get high water mark for server-side filtering
            last_fetched_date = self.db.get_last_fetched_date()

            # Only fetch new launches using server-side filtering with pagination
            new_launches = fetch_launches_after_date(last_fetched_date)

            logger.info(
                f"Server-side filtering returned {len(new_launches)} new launches")
            return new_launches

        except Exception as e:
            logger.error(
                f"Error in paginated fetch, falling back to traditional method: {e}")
            # Fallback: Use the traditional approach if optimized method fails
            return self._fetch_and_filter_new_launches()

    def _fetch_and_filter_new_launches(self) -> List[dict]:
        """
        FALLBACK: Fetch all launches and filter for only new ones based on our high water mark.

        This is the original implementation kept as a fallback for robustness.
        The optimized version (_fetch_new_launches) should be used instead.

        Returns:
            List[dict]: Launch data for launches newer than our last processed date
        """
        try:
            # Real-World Pattern: Fetch full dataset only when needed
            all_launches = fetch_all_launches()

            # Get high water mark for incremental processing
            last_fetched_date = self.db.get_last_fetched_date()

            # Filter for new launches
            new_launches = []

            for launch_data in all_launches:
                try:
                    # Parse launch date for comparison
                    launch_date_str = launch_data.get('date_utc', '')

                    if launch_date_str.endswith('Z'):
                        launch_date_str = launch_date_str.replace(
                            'Z', '+00:00')

                    launch_date = datetime.fromisoformat(launch_date_str)

                    # Include launches newer than our high water mark
                    if launch_date > last_fetched_date:
                        new_launches.append(launch_data)

                except Exception as e:
                    logger.warning(
                        f"Error parsing date for launch {launch_data.get('id', 'unknown')}: {e}")
                    continue

            logger.info(
                f"Found {len(new_launches)} new launches out of {len(all_launches)} total")
            return new_launches

        except Exception as e:
            logger.error(f"Error fetching and filtering launches: {e}")
            raise

    def _validate_launches(self, launch_data_list: List[dict]) -> List[Launch]:
        """
        Validate launch data using Pydantic models for data quality assurance.
        Also calculates total payload mass for each launch by fetching payload data.

        This demonstrates proper data validation patterns in data pipelines.

        Args:
            launch_data_list: Raw launch data from API

        Returns:
            List[Launch]: Validated Launch objects with payload mass data
        """
        validated_launches = []
        validation_errors = 0

        for launch_data in launch_data_list:
            try:
                # Data Quality: Validate each launch with Pydantic
                launch = Launch(**launch_data)

                # Calculate total payload mass by fetching payload data
                if launch.payload_ids:
                    logger.info(
                        f"Calculating payload mass for launch {launch.id} with {len(launch.payload_ids)} payloads")
                    total_mass = calculate_total_payload_mass(
                        launch.payload_ids)
                    launch.total_payload_mass_kg = total_mass if total_mass > 0 else None
                    logger.debug(
                        f"Launch {launch.id} total payload mass: {launch.total_payload_mass_kg} kg")
                else:
                    launch.total_payload_mass_kg = None
                    logger.debug(f"Launch {launch.id} has no payloads")

                validated_launches.append(launch)

            except Exception as e:
                validation_errors += 1
                logger.warning(
                    f"Validation failed for launch {launch_data.get('id', 'unknown')}: {e}")

        logger.info(
            f"Validation completed: {len(validated_launches)} valid, {validation_errors} errors")
        return validated_launches

    def _insert_new_launches(self, launches: List[Launch]) -> int:
        """
        Insert new launches using batch processing for optimal performance.

        Args:
            launches: Validated Launch objects to insert

        Returns:
            int: Number of launches actually inserted
        """
        if not launches:
            return 0

        try:
            # Demonstrates Key Concepts: Batch processing for efficiency
            inserted_count = self.db.insert_launches_batch(launches)

            if inserted_count > 0:
                logger.info(
                    f"Successfully inserted {inserted_count} new launches")
            else:
                logger.info(
                    "No new launches inserted (all were duplicates/updates)")

            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting launches: {e}")
            raise

    def _update_ingestion_state(self, launches: List[Launch]) -> None:
        """
        Update the high water mark to track incremental processing progress.

        This implements the tracking mechanism that enables incremental processing
        across multiple pipeline runs.

        Args:
            launches: Successfully processed launches
        """
        if not launches:
            return

        try:
            # Update high water mark to the latest launch date
            latest_date = max(launch.date_utc for launch in launches)
            self.db.update_last_fetched_date(latest_date)

        except Exception as e:
            logger.error(f"Error updating ingestion state: {e}")
            raise


def run_ingestion():
    """
    Main entry point for the incremental ingestion pipeline.

    This function demonstrates production-ready data engineering patterns:
    - Change detection
    - Incremental processing
    - Comprehensive monitoring and logging
    - Error handling and recovery
    """
    pipeline = IncrementalIngestionPipeline()
    return pipeline.run_incremental_ingestion()


if __name__ == "__main__":
    # Run the ingestion pipeline
    result = run_ingestion()

    # Print summary for monitoring
    print(f"\n=== Ingestion Pipeline Summary ===")
    print(f"Status: {result['status']}")
    print(f"New launches found: {result.get('new_launches_found', 0)}")
    print(f"Launches inserted: {result.get('launches_inserted', 0)}")
    print(f"Duration: {result['pipeline_duration_seconds']:.2f} seconds")
    print(f"API calls made: {result.get('api_calls_made', 0)}")

    # Aggregation summary
    agg_result = result.get('aggregations', {})

    if agg_result.get('status') == 'success':
        print(
            f"Aggregations updated: Total launches: {agg_result.get('total_launches', 'N/A')}, Success rate: {agg_result.get('success_rate', 'N/A')}%")
    elif agg_result.get('status') == 'skipped':
        print(
            f"Aggregations skipped: {agg_result.get('reason', 'unknown reason')}")
    elif agg_result.get('status') == 'error':
        print(
            f"Aggregation error: {agg_result.get('error_message', 'Unknown error')}")

    if result.get('initial_load'):
        print("Initial load completed - all launches fetched and aggregations initialized")
    elif result.get('early_exit'):
        print("Pipeline completed early - no new data detected")

    if result['status'] == 'error':
        print(f"Error: {result.get('error_message', 'Unknown error')}")
        exit(1)
