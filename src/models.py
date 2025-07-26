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
