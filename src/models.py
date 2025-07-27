from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import List, Optional, Any


class Launch(BaseModel):
    """
    Pydantic model for SpaceX launch data validation and serialization.

    This model ensures data quality and provides type safety for our
    data pipeline operations.
    """
    id: str = Field(..., alias="id", description="Unique launch identifier")
    name: Optional[str] = Field(None, alias="name", description="Mission name")
    date_utc: datetime = Field(..., alias="date_utc",
                               description="Launch date in UTC")
    success: Optional[bool] = Field(
        None, alias="success", description="Launch success status")
    payload_ids: Optional[List[str]] = Field(
        default_factory=list, alias="payloads", description="List of payload IDs")
    total_payload_mass_kg: Optional[float] = Field(
        None, description="Total payload mass in kilograms")
    launchpad_id: Optional[str] = Field(
        None, alias="launchpad", description="Launchpad identifier")
    static_fire_date_utc: Optional[datetime] = Field(
        None, alias="static_fire_date_utc", description="Static fire test date")

    class Config:
        """Pydantic configuration."""
        validate_by_name = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

    @field_validator('date_utc', mode='before')
    def parse_date_utc(cls, v: Any) -> datetime:
        """Parse date_utc from various formats."""
        if isinstance(v, datetime):
            return v

        if isinstance(v, str):
            # Handle ISO format with 'Z' suffix
            if v.endswith('Z'):
                v = v.replace('Z', '+00:00')

            return datetime.fromisoformat(v)

        raise ValueError(f"Invalid date format: {v}")

    @field_validator('static_fire_date_utc', mode='before')
    def parse_static_fire_date(cls, v: Any) -> Optional[datetime]:
        """Parse static_fire_date_utc from various formats."""
        if v is None:
            return None

        if isinstance(v, datetime):
            return v

        if isinstance(v, str):
            # Handle ISO format with 'Z' suffix
            if v.endswith('Z'):
                v = v.replace('Z', '+00:00')

            return datetime.fromisoformat(v)

        raise ValueError(f"Invalid static fire date format: {v}")

    @field_validator('payload_ids', mode='before')
    def ensure_payload_list(cls, v: Any) -> List[str]:
        """Ensure payload_ids is always a list."""
        if v is None:
            return []

        if isinstance(v, list):
            return v

        if isinstance(v, str):
            return [v]

        return list(v)


class LaunchAggregations(BaseModel):
    """
    Pydantic model for launch aggregation data.

    Supports time-series tracking for trend analysis over time.
    """
    id: Optional[int] = None  # Auto-generated for new records
    total_launches: int = 0
    total_successful_launches: int = 0
    total_failed_launches: int = 0
    success_rate: Optional[float] = None
    earliest_launch_date: Optional[datetime] = None
    latest_launch_date: Optional[datetime] = None
    total_launch_sites: int = 0
    average_payload_mass_kg: Optional[float] = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now())
    last_processed_launch_date: Optional[datetime] = None
    # Time-series fields
    snapshot_type: str = "incremental"  # 'initial', 'incremental', 'manual'
    launches_added_in_batch: int = 0
    pipeline_run_id: Optional[str] = None

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

    def calculate_success_rate(self) -> Optional[float]:
        """Calculate success rate from total and successful launches."""
        if self.total_launches == 0:
            return None
        return round((self.total_successful_launches / self.total_launches) * 100, 2)
