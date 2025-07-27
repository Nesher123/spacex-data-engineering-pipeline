import logging
import uuid
from datetime import datetime
from typing import List, Optional, Set
from sqlalchemy import text
from models import Launch, LaunchAggregations
from database import Database

# Get logger
logger = logging.getLogger(__name__)


class AggregationService:
    """
    Service for maintaining launch aggregations using time-series approach.

    This service creates a new aggregation record for each update,
    enabling trend analysis and historical tracking over time.
    """

    def __init__(self, database: Optional[Database] = None):
        self.db = database or Database()

    def update_aggregations_for_new_launches(self, new_launches: List[Launch], pipeline_run_id: Optional[str] = None) -> dict:
        """
        Create new aggregation record based on newly ingested launches.

        This creates a time-series record showing how metrics evolved.

        Args:
            new_launches: List of newly ingested Launch objects
            pipeline_run_id: Optional identifier for tracking pipeline runs

        Returns:
            dict: Summary of aggregation update results
        """
        if not new_launches:
            logger.info("No new launches to process for aggregations")
            return {
                'status': 'success',
                'launches_processed': 0,
                'aggregations_updated': False,
                'method': 'time_series_incremental'
            }

        logger.info(
            f"Creating new aggregation record for {len(new_launches)} new launches")

        try:
            # Generate pipeline run ID if not provided
            if not pipeline_run_id:
                pipeline_run_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

            # Get current aggregations (latest record)
            current_agg = self._get_latest_aggregations()

            # Calculate new aggregation state
            updated_agg = self._calculate_incremental_updates(
                current_agg, new_launches)

            # Set time-series specific fields
            updated_agg.snapshot_type = "incremental"
            updated_agg.launches_added_in_batch = len(new_launches)
            updated_agg.pipeline_run_id = pipeline_run_id
            updated_agg.updated_at = datetime.now()

            # Insert new aggregation record (time-series approach)
            self._insert_new_aggregation_record(updated_agg)

            logger.info(f"Successfully created aggregation record: "
                        f"Total launches: {updated_agg.total_launches}, "
                        f"Success rate: {updated_agg.success_rate}%, "
                        f"Run ID: {pipeline_run_id}")

            return {
                'status': 'success',
                'launches_processed': len(new_launches),
                'aggregations_updated': True,
                'method': 'time_series_incremental',
                'total_launches': updated_agg.total_launches,
                'success_rate': updated_agg.success_rate,
                'pipeline_run_id': pipeline_run_id,
                'aggregation_id': updated_agg.id
            }

        except Exception as e:
            logger.error(f"Failed to create aggregation record: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'launches_processed': len(new_launches),
                'aggregations_updated': False,
                'method': 'time_series_incremental'
            }

    def initialize_aggregations_from_scratch(self, pipeline_run_id: Optional[str] = None) -> dict:
        """
        Create initial aggregation record by calculating from all existing launches.

        This creates the first time-series record for initial setup.

        Args:
            pipeline_run_id: Optional identifier for tracking pipeline runs

        Returns:
            dict: Summary of initialization results
        """
        logger.info("Creating initial aggregation record from all launches")

        try:
            # Generate pipeline run ID if not provided
            if not pipeline_run_id:
                pipeline_run_id = f"initial_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

            # Calculate aggregations from all existing launches
            aggregations = self._calculate_aggregations_from_all_launches()

            # Set time-series specific fields for initial load
            aggregations.snapshot_type = "initial"
            aggregations.launches_added_in_batch = aggregations.total_launches
            aggregations.pipeline_run_id = pipeline_run_id
            aggregations.updated_at = datetime.now()

            # Insert initial aggregation record
            self._insert_new_aggregation_record(aggregations)

            logger.info(f"Successfully created initial aggregation record: "
                        f"Total launches: {aggregations.total_launches}, "
                        f"Success rate: {aggregations.success_rate}%, "
                        f"Run ID: {pipeline_run_id}")

            return {
                'status': 'success',
                'method': 'time_series_initial',
                'total_launches': aggregations.total_launches,
                'success_rate': aggregations.success_rate,
                'pipeline_run_id': pipeline_run_id,
                'aggregation_id': aggregations.id
            }

        except Exception as e:
            logger.error(f"Failed to create initial aggregation record: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'method': 'time_series_initial'
            }

    def _get_latest_aggregations(self) -> LaunchAggregations:
        """
        Get the most recent aggregation record.

        Returns:
            LaunchAggregations: Latest aggregation state
        """
        with self.db.Session() as session:
            result = session.execute(text("""
                SELECT 
                    id, total_launches, total_successful_launches, total_failed_launches,
                    success_rate, earliest_launch_date, latest_launch_date,
                    total_launch_sites, average_payload_mass_kg, average_delay_hours, updated_at, last_processed_launch_date,
                    snapshot_type, launches_added_in_batch, pipeline_run_id
                FROM launch_aggregations 
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
            """))

            row = result.fetchone()

            if row:
                return LaunchAggregations(
                    id=row[0],
                    total_launches=row[1] or 0,
                    total_successful_launches=row[2] or 0,
                    total_failed_launches=row[3] or 0,
                    success_rate=float(row[4]) if row[4] else None,
                    earliest_launch_date=row[5],
                    latest_launch_date=row[6],
                    total_launch_sites=row[7] or 0,
                    average_payload_mass_kg=float(row[8]) if row[8] else None,
                    average_delay_hours=float(row[9]) if row[9] else None,
                    updated_at=row[10] or datetime.now(),
                    last_processed_launch_date=row[11],
                    snapshot_type=row[12] or "unknown",
                    launches_added_in_batch=row[13] or 0,
                    pipeline_run_id=row[14]
                )
            else:
                # Return empty aggregations if none exist
                return LaunchAggregations()

    def get_aggregation_history(self, limit: int = 10) -> List[LaunchAggregations]:
        """
        Get historical aggregation records for trend analysis.

        Args:
            limit: Maximum number of records to return

        Returns:
            List[LaunchAggregations]: Historical aggregation records
        """
        with self.db.Session() as session:
            result = session.execute(text("""
                SELECT 
                    id, total_launches, total_successful_launches, total_failed_launches,
                    success_rate, earliest_launch_date, latest_launch_date,
                    total_launch_sites, average_payload_mass_kg, average_delay_hours, updated_at, last_processed_launch_date,
                    snapshot_type, launches_added_in_batch, pipeline_run_id
                FROM launch_aggregations 
                ORDER BY updated_at DESC, id DESC
                LIMIT :limit
            """), {"limit": limit})

            history = []
            for row in result.fetchall():
                history.append(LaunchAggregations(
                    id=row[0],
                    total_launches=row[1] or 0,
                    total_successful_launches=row[2] or 0,
                    total_failed_launches=row[3] or 0,
                    success_rate=float(row[4]) if row[4] else None,
                    earliest_launch_date=row[5],
                    latest_launch_date=row[6],
                    total_launch_sites=row[7] or 0,
                    average_payload_mass_kg=float(row[8]) if row[8] else None,
                    average_delay_hours=float(row[9]) if row[9] else None,
                    updated_at=row[10] or datetime.now(),
                    last_processed_launch_date=row[11],
                    snapshot_type=row[12] or "unknown",
                    launches_added_in_batch=row[13] or 0,
                    pipeline_run_id=row[14]
                ))

            return history

    def _calculate_incremental_updates(
        self,
        current_agg: LaunchAggregations,
        new_launches: List[Launch]
    ) -> LaunchAggregations:
        """
        Calculate new aggregation state based on current state and new launches.

        Args:
            current_agg: Current aggregation state
            new_launches: New launches to process

        Returns:
            LaunchAggregations: New aggregation state
        """
        # Start with current aggregations (copy values)
        updated_agg = LaunchAggregations(
            total_launches=current_agg.total_launches,
            total_successful_launches=current_agg.total_successful_launches,
            total_failed_launches=current_agg.total_failed_launches,
            earliest_launch_date=current_agg.earliest_launch_date,
            latest_launch_date=current_agg.latest_launch_date,
            total_launch_sites=current_agg.total_launch_sites,
            average_payload_mass_kg=current_agg.average_payload_mass_kg,
            average_delay_hours=current_agg.average_delay_hours
        )

        # Track unique launch sites for incremental counting
        new_launch_sites: Set[str] = set()

        # Process each new launch
        for launch in new_launches:
            # Increment total launches
            updated_agg.total_launches += 1

            # Count successful/failed launches
            if launch.success is True:
                updated_agg.total_successful_launches += 1
            elif launch.success is False:
                updated_agg.total_failed_launches += 1

            # Update date ranges
            if (updated_agg.earliest_launch_date is None or
                    launch.date_utc < updated_agg.earliest_launch_date):
                updated_agg.earliest_launch_date = launch.date_utc

            if (updated_agg.latest_launch_date is None or
                    launch.date_utc > updated_agg.latest_launch_date):
                updated_agg.latest_launch_date = launch.date_utc

            # Track launch sites
            if launch.launchpad_id:
                new_launch_sites.add(launch.launchpad_id)

        # Update launch sites count (full recount for accuracy)
        updated_agg.total_launch_sites = self._count_unique_launch_sites()

        # Calculate success rate
        updated_agg.success_rate = updated_agg.calculate_success_rate()

        # Calculate average payload mass (full recount for accuracy)
        updated_agg.average_payload_mass_kg = self._calculate_average_payload_mass()

        # Calculate average delay hours (full recount for accuracy)
        updated_agg.average_delay_hours = self._calculate_average_delay_hours()

        # Update processed date
        if new_launches:
            updated_agg.last_processed_launch_date = max(
                launch.date_utc for launch in new_launches
            )

        return updated_agg

    def _calculate_aggregations_from_all_launches(self) -> LaunchAggregations:
        """
        Calculate aggregations from all launches in the database.

        Used for initialization and full refresh.

        Returns:
            LaunchAggregations: Complete aggregation state
        """
        with self.db.Session() as session:
            # Get aggregation data using SQL for efficiency
            result = session.execute(text("""
                SELECT 
                    COUNT(*) as total_launches,
                    COUNT(CASE WHEN success = true THEN 1 END) as successful_launches,
                    COUNT(CASE WHEN success = false THEN 1 END) as failed_launches,
                    MIN(date_utc) as earliest_launch,
                    MAX(date_utc) as latest_launch,
                    COUNT(DISTINCT launchpad_id) as unique_launch_sites,
                    AVG(CASE WHEN total_payload_mass_kg > 0 THEN total_payload_mass_kg END) as avg_payload_mass,
                    AVG(CASE 
                        WHEN static_fire_date_utc IS NOT NULL 
                         AND static_fire_date_utc <= date_utc 
                        THEN EXTRACT(EPOCH FROM (date_utc - static_fire_date_utc)) / 3600 
                        END) as avg_delay_hours
                FROM raw_launches
            """))

            row = result.fetchone()

            if row and row[0] > 0:  # Check if we have any launches
                total_launches = row[0]
                successful_launches = row[1] or 0
                failed_launches = row[2] or 0

                # Calculate success rate
                success_rate = None
                if total_launches > 0:
                    success_rate = round(
                        (successful_launches / total_launches) * 100, 2)
                # NOTE: Some 'success' values are None, so we don't count them as successful nor failed

                return LaunchAggregations(
                    total_launches=total_launches,
                    total_successful_launches=successful_launches,
                    total_failed_launches=failed_launches,
                    success_rate=success_rate,
                    earliest_launch_date=row[3],
                    latest_launch_date=row[4],
                    total_launch_sites=row[5] or 0,
                    average_payload_mass_kg=float(row[6]) if row[6] else None,
                    average_delay_hours=float(row[7]) if row[7] else None,
                    last_processed_launch_date=row[4]  # Latest launch date
                )
            else:
                # Return empty aggregations if no launches
                return LaunchAggregations()

    def _count_unique_launch_sites(self) -> int:
        """
        Count unique launch sites from all launches.

        Returns:
            int: Number of unique launch sites
        """
        with self.db.Session() as session:
            result = session.execute(text("""
                SELECT COUNT(DISTINCT launchpad_id) 
                FROM raw_launches 
                WHERE launchpad_id IS NOT NULL
            """))
            return result.scalar() or 0

    def _calculate_average_payload_mass(self) -> Optional[float]:
        """
        Calculate average payload mass from all launches.

        Returns:
            float: Average payload mass in kg, or None if no valid data
        """
        with self.db.Session() as session:
            result = session.execute(text("""
                SELECT AVG(total_payload_mass_kg) 
                FROM raw_launches 
                WHERE total_payload_mass_kg IS NOT NULL AND total_payload_mass_kg > 0
            """))
            avg_mass = result.scalar()
            return float(avg_mass) if avg_mass else None

    def _calculate_average_delay_hours(self) -> Optional[float]:
        """
        Calculate average delay hours between static fire test and actual launch.

        Returns:
            float: Average delay in hours, or None if no valid data
        """
        with self.db.Session() as session:
            result = session.execute(text("""
                SELECT AVG(EXTRACT(EPOCH FROM (date_utc - static_fire_date_utc)) / 3600) 
                FROM raw_launches 
                WHERE static_fire_date_utc IS NOT NULL 
                  AND date_utc IS NOT NULL 
                  AND static_fire_date_utc <= date_utc
            """))
            avg_delay = result.scalar()
            return float(avg_delay) if avg_delay else None

    def _insert_new_aggregation_record(self, aggregations: LaunchAggregations) -> None:
        """
        Insert new aggregation record (time-series approach).

        Args:
            aggregations: LaunchAggregations object to insert
        """
        with self.db.Session() as session:
            try:
                result = session.execute(text("""
                    INSERT INTO launch_aggregations (
                        total_launches, total_successful_launches, total_failed_launches,
                        success_rate, earliest_launch_date, latest_launch_date,
                        total_launch_sites, average_payload_mass_kg, average_delay_hours, updated_at, last_processed_launch_date,
                        snapshot_type, launches_added_in_batch, pipeline_run_id
                    ) VALUES (
                        :total_launches, :total_successful_launches, :total_failed_launches,
                        :success_rate, :earliest_launch_date, :latest_launch_date,
                        :total_launch_sites, :average_payload_mass_kg, :average_delay_hours, :updated_at, :last_processed_launch_date,
                        :snapshot_type, :launches_added_in_batch, :pipeline_run_id
                    ) RETURNING id
                """), {
                    'total_launches': aggregations.total_launches,
                    'total_successful_launches': aggregations.total_successful_launches,
                    'total_failed_launches': aggregations.total_failed_launches,
                    'success_rate': aggregations.success_rate,
                    'earliest_launch_date': aggregations.earliest_launch_date,
                    'latest_launch_date': aggregations.latest_launch_date,
                    'total_launch_sites': aggregations.total_launch_sites,
                    'average_payload_mass_kg': aggregations.average_payload_mass_kg,
                    'average_delay_hours': aggregations.average_delay_hours,
                    'updated_at': aggregations.updated_at,
                    'last_processed_launch_date': aggregations.last_processed_launch_date,
                    'snapshot_type': aggregations.snapshot_type,
                    'launches_added_in_batch': aggregations.launches_added_in_batch,
                    'pipeline_run_id': aggregations.pipeline_run_id
                })

                # Get the generated ID
                new_id = result.scalar()
                aggregations.id = new_id

                session.commit()
                logger.debug(f"Aggregation record inserted with ID: {new_id}")

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to insert aggregation record: {e}")
                raise

    def get_aggregations(self) -> LaunchAggregations:
        """
        Get current aggregations (latest record) for external use.

        Returns:
            LaunchAggregations: Latest aggregation state
        """
        return self._get_latest_aggregations()
