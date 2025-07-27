import os
import logging
import json
from datetime import datetime, timezone
from typing import Optional, List
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Launch

# Get logger without configuring it (let main pipeline handle configuration)
logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        # Database connection configuration
        # Use localhost for local development, postgres for containerized deployment
        db_host = os.getenv('POSTGRES_HOST', 'localhost')
        db_port = os.getenv('POSTGRES_PORT', '5432')
        db_user = os.getenv('POSTGRES_USER', 'postgres')
        # Match docker-compose.yml
        db_password = os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')
        db_name = os.getenv('POSTGRES_DB', 'mydatabase')

        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        logger.info(f"Connecting to database at {db_host}:{db_port}")

        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def get_last_fetched_date(self) -> datetime:
        """
        Get the timestamp of the last successful ingestion.

        This is used to determine the high water mark for incremental processing.

        Returns:
            datetime: Last fetched date (timezone-aware UTC), defaults to year 1970 if no prior ingestion
        """
        with self.Session() as session:
            result = session.execute(text(
                "SELECT last_fetched_date FROM ingestion_state ORDER BY updated_at DESC LIMIT 1"))
            last_date = result.scalar()

            if last_date:
                # TIMESTAMPTZ automatically returns timezone-aware datetime
                logger.info(f"Last fetched date from database: {last_date}")
                return last_date
            else:
                # Return timezone-aware default date
                default_date = datetime(1970, 1, 1, tzinfo=timezone.utc)
                logger.info(
                    f"No previous ingestion found, using default: {default_date}")
                return default_date

    def get_latest_launch_from_db(self) -> Optional[Launch]:
        """
        Get the most recent launch stored in the database.

        This is used for change detection to compare against the API's latest launch.

        Returns:
            Launch: Most recent launch object, or None if database is empty
        """
        with self.Session() as session:
            result = session.execute(text("""
                SELECT launch_id, mission_name, date_utc, success, payload_ids, total_payload_mass_kg, launchpad_id, static_fire_date_utc
                FROM raw_launches 
                ORDER BY date_utc DESC 
                LIMIT 1
            """))

            row = result.fetchone()

            if row:
                # Convert row to dict for Launch model
                # Deserialize JSONB payload_ids back to Python list
                payload_ids = json.loads(row[4]) if row[4] else []

                launch_data = {
                    'id': row[0],
                    'name': row[1],
                    # TIMESTAMPTZ automatically returns timezone-aware datetime
                    'date_utc': row[2],
                    'success': row[3],
                    'payloads': payload_ids,
                    'total_payload_mass_kg': row[5],
                    'launchpad': row[6],
                    # TIMESTAMPTZ automatically returns timezone-aware datetime
                    'static_fire_date_utc': row[7]
                }
                launch = Launch(**launch_data)
                logger.info(
                    f"Latest launch in database: {launch.name} ({launch.id}) at {launch.date_utc}")
                return launch
            else:
                logger.info("No launches found in database")
                return None

    def insert_launches_batch(self, launches: List[Launch]) -> int:
        """
        Insert multiple launches in a single transaction for better performance.

        Args:
            launches: List of Launch objects to insert

        Returns:
            int: Number of launches actually inserted (excluding duplicates)
        """
        if not launches:
            logger.info("No launches to insert")
            return 0

        with self.Session() as session:
            try:
                # Prepare batch data
                launch_data = []

                for launch in launches:
                    launch_data.append({
                        'id': launch.id,
                        'name': launch.name,
                        'date_utc': launch.date_utc,
                        'success': launch.success,
                        'payload_ids': json.dumps(launch.payload_ids) if launch.payload_ids else json.dumps([]),
                        'total_payload_mass_kg': launch.total_payload_mass_kg,
                        'launchpad_id': launch.launchpad_id,
                        'static_fire_date_utc': launch.static_fire_date_utc
                    })

                # Get count before insert
                count_before = session.execute(
                    text("SELECT COUNT(*) FROM raw_launches")).scalar()

                # Batch insert with conflict handling
                session.execute(
                    text("""
                        INSERT INTO raw_launches (launch_id, mission_name, date_utc, success, payload_ids, total_payload_mass_kg, launchpad_id, static_fire_date_utc)
                        VALUES (:id, :name, :date_utc, :success, :payload_ids, :total_payload_mass_kg, :launchpad_id, :static_fire_date_utc)
                        ON CONFLICT (launch_id) DO NOTHING
                    """),
                    launch_data
                )

                # Get count after insert to determine how many were actually inserted
                count_after = session.execute(
                    text("SELECT COUNT(*) FROM raw_launches")).scalar()
                inserted_count = count_after - count_before

                session.commit()
                logger.info(
                    f"Batch insert completed: {inserted_count} new launches inserted out of {len(launches)} processed")
                return inserted_count

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to batch insert launches: {e}")
                raise

    def update_last_fetched_date(self, date_utc: datetime) -> None:
        """
        Update the last fetched date to track incremental processing progress.

        Args:
            date_utc: The timestamp to record as the last successful fetch
        """
        with self.Session() as session:
            try:
                session.execute(
                    text(
                        "INSERT INTO ingestion_state (last_fetched_date) VALUES (:date)"),
                    {"date": date_utc}
                )
                session.commit()
                logger.info(f"Updated last fetched date to: {date_utc}")
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to update last fetched date: {e}")
                raise

    def is_new_data_available(self, api_latest_launch: dict) -> bool:
        """
        Determine if new data is available by comparing API's latest launch
        with our database's latest launch.

        This is the core of our change detection optimization.

        Args:
            api_latest_launch: Latest launch data from the API

        Returns:
            bool: True if new data should be ingested, False otherwise
        """
        try:
            # Get our latest launch from database
            db_latest_launch = self.get_latest_launch_from_db()

            # If database is empty, we definitely need to ingest
            if db_latest_launch is None:
                logger.info("Database is empty - new data ingestion required")
                return True

            # Parse API launch date
            api_launch_date = datetime.fromisoformat(
                api_latest_launch['date_utc'].replace('Z', '+00:00'))
            api_launch_id = api_latest_launch['id']

            # Compare by date first (most common case)
            if api_launch_date > db_latest_launch.date_utc:
                logger.info(
                    f"New launch detected by date: API {api_launch_date} > DB {db_latest_launch.date_utc}")
                return True

            # If dates are the same, compare by ID (handles updates to same launch)
            if api_launch_date == db_latest_launch.date_utc and api_launch_id != db_latest_launch.id:
                logger.info(
                    f"New launch detected by ID: API {api_launch_id} != DB {db_latest_launch.id}")
                return True

            # No new data detected
            logger.info("No new data detected - skipping full ingestion")
            return False

        except Exception as e:
            logger.error(f"Error in change detection: {e}")
            # When in doubt, assume new data is available (fail-safe approach)
            return True
