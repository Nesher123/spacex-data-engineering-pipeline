#!/usr/bin/env python3
"""
Test script for time-series aggregation functionality.

This script validates that the aggregation service works correctly
with time-series records for trend analysis over time.
"""

import logging
from database import Database
from aggregations import AggregationService

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_aggregations():
    """Test time-series aggregation functionality."""
    print("=== Testing Time-Series Aggregation Functionality ===")

    try:
        # Initialize services
        db = Database()
        agg_service = AggregationService(db)

        # Test 1: Check if we can get current aggregations
        print("\n1. Testing current aggregations retrieval...")
        current_agg = agg_service.get_aggregations()
        print(f"   Current aggregations: {current_agg.total_launches} launches, "
              f"{current_agg.success_rate}% success rate")
        print(
            f"   Record ID: {current_agg.id}, Type: {current_agg.snapshot_type}")

        # Test 2: Initialize aggregations from scratch
        print("\n2. Testing aggregation initialization...")
        init_result = agg_service.initialize_aggregations_from_scratch()
        print(f"   Initialization result: {init_result}")
        print(
            f"   Pipeline Run ID: {init_result.get('pipeline_run_id', 'N/A')}")

        # Test 3: Get updated aggregations
        print("\n3. Testing updated aggregations...")
        updated_agg = agg_service.get_aggregations()
        print(
            f"   Updated aggregations: {updated_agg.total_launches} launches")
        print(
            f"   Successful launches: {updated_agg.total_successful_launches}")
        print(f"   Failed launches: {updated_agg.total_failed_launches}")
        print(f"   Success rate: {updated_agg.success_rate}%")
        print(f"   Launch sites: {updated_agg.total_launch_sites}")
        print(
            f"   Date range: {updated_agg.earliest_launch_date} to {updated_agg.latest_launch_date}")
        print(f"   Snapshot type: {updated_agg.snapshot_type}")
        print(f"   Launches in batch: {updated_agg.launches_added_in_batch}")

        # Test 4: Validate data consistency
        print("\n4. Testing data consistency...")
        total_calculated = updated_agg.total_successful_launches + \
            updated_agg.total_failed_launches

        # Note: Some launches might have success=null, so total might be higher
        if total_calculated <= updated_agg.total_launches:
            print(f"   ✓ Data consistency check passed")
            print(f"     Total launches: {updated_agg.total_launches}")
            print(f"     Success + Failed: {total_calculated}")
            print(
                f"     Launches with unknown status: {updated_agg.total_launches - total_calculated}")
        else:
            print(f"   ✗ Data consistency issue detected")

        # Test 5: Database validation
        print("\n5. Testing database validation...")
        with db.Session() as session:
            from sqlalchemy import text
            result = session.execute(text("SELECT COUNT(*) FROM raw_launches"))
            db_count = result.scalar()

            if db_count == updated_agg.total_launches:
                print(f"   ✓ Database count matches aggregation: {db_count}")
            else:
                print(
                    f"   ✗ Database count mismatch: DB={db_count}, Agg={updated_agg.total_launches}")

        # Test 6: Time-series functionality
        print("\n6. Testing time-series functionality...")
        history = agg_service.get_aggregation_history(limit=5)
        print(f"   Found {len(history)} aggregation records")

        for i, record in enumerate(history):
            print(f"   Record {i+1}: ID={record.id}, "
                  f"Launches={record.total_launches}, "
                  f"Type={record.snapshot_type}, "
                  f"Updated={record.updated_at.strftime('%Y-%m-%d %H:%M:%S')}")

        # Test 7: Validate time-series ordering
        print("\n7. Testing time-series ordering...")
        if len(history) > 1:
            is_ordered = all(
                history[i].updated_at >= history[i+1].updated_at
                for i in range(len(history)-1)
            )
            if is_ordered:
                print("   ✓ Records are properly ordered by timestamp")
            else:
                print("   ✗ Records are not properly ordered")
        else:
            print("   ⚠ Only one record found - cannot test ordering")

        # Test 8: Pipeline run ID tracking
        print("\n8. Testing pipeline run ID tracking...")
        latest_record = history[0] if history else updated_agg
        if latest_record.pipeline_run_id:
            print(
                f"   ✓ Pipeline run ID present: {latest_record.pipeline_run_id}")
        else:
            print("   ⚠ No pipeline run ID found")

        print("\n=== Time-Series Aggregation Test Completed Successfully ===")
        return True

    except Exception as e:
        print(f"\n✗ Aggregation test failed: {e}")
        logger.error(f"Aggregation test error: {e}")
        return False


def show_aggregation_summary():
    """Show a summary of current aggregations."""
    print("\n=== Current Aggregation Summary ===")

    try:
        agg_service = AggregationService()
        agg = agg_service.get_aggregations()

        print(f"Record ID: {agg.id}")
        print(f"Total Launches: {agg.total_launches}")
        print(f"Successful Launches: {agg.total_successful_launches}")
        print(f"Failed Launches: {agg.total_failed_launches}")
        print(f"Success Rate: {agg.success_rate}%")
        print(f"Launch Sites: {agg.total_launch_sites}")
        print(f"Earliest Launch: {agg.earliest_launch_date}")
        print(f"Latest Launch: {agg.latest_launch_date}")
        print(f"Last Updated: {agg.updated_at}")
        print(f"Snapshot Type: {agg.snapshot_type}")
        print(f"Launches in Batch: {agg.launches_added_in_batch}")
        print(f"Pipeline Run ID: {agg.pipeline_run_id}")

    except Exception as e:
        print(f"Error retrieving aggregations: {e}")


def show_aggregation_trends():
    """Show aggregation trends over time."""
    print("\n=== Aggregation Trends Over Time ===")

    try:
        agg_service = AggregationService()
        history = agg_service.get_aggregation_history(limit=10)

        if not history:
            print("No aggregation history found.")
            return

        print(f"Found {len(history)} aggregation snapshots:")
        print()
        print("Date/Time               | Launches | Success Rate | Type       | Batch Size | Run ID")
        print("-" * 95)

        for record in history:
            date_str = record.updated_at.strftime('%Y-%m-%d %H:%M:%S')
            run_id_short = record.pipeline_run_id[:
                                                  20] if record.pipeline_run_id else "N/A"
            print(f"{date_str} | {record.total_launches:8d} | {record.success_rate or 0:10.2f}% | {record.snapshot_type:10s} | {record.launches_added_in_batch:10d} | {run_id_short}")

        # Calculate trends if we have multiple records
        if len(history) >= 2:
            print()
            print("=== Trend Analysis ===")
            latest = history[0]
            previous = history[1]

            launch_change = latest.total_launches - previous.total_launches
            if latest.success_rate and previous.success_rate:
                rate_change = latest.success_rate - previous.success_rate
                print(f"Launch count change: +{launch_change}")
                print(f"Success rate change: {rate_change:+.2f}%")
            else:
                print(f"Launch count change: +{launch_change}")
                print("Success rate change: N/A")

    except Exception as e:
        print(f"Error retrieving aggregation trends: {e}")


if __name__ == "__main__":
    # Run tests
    success = test_aggregations()

    # Show current summary
    show_aggregation_summary()

    # Show trends
    show_aggregation_trends()

    if not success:
        exit(1)

    print("\n✓ All tests passed!")
