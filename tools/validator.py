from datetime import datetime
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo

import annotated_types
from google.protobuf import json_format
from pydantic import AfterValidator, BaseModel, Field, StringConstraints


def _timezone_validator(v: str | None) -> str | None:
    if v is None:
        return None
    try:
        ZoneInfo(v)
    except Exception:
        raise ValueError("The timezone is invalid")
    return v


class Event(BaseModel):
    event_index: Annotated[
        str,
        StringConstraints(min_length=1, max_length=36),
        Field(),
    ]
    timestamp: datetime = Field()
    type: Annotated[
        str,
        StringConstraints(pattern=r"[a-zA-Z0-9_\-]+(\.[a-zA-Z0-9_\-]+)*", min_length=1, max_length=32),
        Field(),
    ]
    labels: list[Annotated[str, StringConstraints(min_length=1, max_length=16)]] | None = Field(
        None,
        max_length=20,
        min_length=1,
    )
    score: float | None = Field(None)
    data: dict[str, Any] | None = Field(None)
    has_picture: bool = Field(False)
    geometry_config_ids: list[int] | None = Field(
        None,
        max_length=8,
        min_length=1,
    )


class Object(BaseModel):
    object_index: Annotated[str, StringConstraints(min_length=1, max_length=36), Field()]
    start_timestamp: datetime = Field()
    end_timestamp: datetime | None = Field(None)
    duration: Annotated[int | None, annotated_types.Interval(ge=0, le=2147483647), Field(None)]
    type: Annotated[
        str,
        StringConstraints(
            pattern=r"^[a-zA-Z0-9_\-]+(\.[a-zA-Z0-9_\-]+)*$",
            min_length=1,
            max_length=32,
        ),
        Field(),
    ]
    labels: list[Annotated[str, StringConstraints(min_length=1, max_length=16)]] | None = Field(
        None,
        max_length=20,
        min_length=1,
    )
    score: float | None = Field(None)
    data: dict[str, Any] | None = Field(None)
    picture_labels: (
        list[Annotated[str, StringConstraints(min_length=1, max_length=16, pattern=r"^[a-zA-Z0-9_\-]+$")]]
        | None
    ) = Field(
        None,
        max_length=5,
        min_length=1,
    )
    has_extra_data: bool = Field(False)
    geometry_config_ids: list[int] | None = Field(
        None,
        max_length=8,
        min_length=1,
    )


class Metric(BaseModel):
    timestamp: datetime = Field()
    units: list[Literal["daily", "hourly", "5minutes"]] = Field(max_length=3, min_length=1)
    label: Annotated[
        str,
        StringConstraints(min_length=1, max_length=16),
        Field(),
    ]
    value: float = Field()
    daily_boundary_timezone: Annotated[
        str | None,
        AfterValidator(_timezone_validator),
        Field(None),
    ]


class DeviceStatus(BaseModel):
    label: Annotated[str, StringConstraints(min_length=1, max_length=16)]
    status: Annotated[str, StringConstraints(min_length=1, max_length=16)]
    timestamp: datetime = Field()
    score: float | None = Field(None)
    geometry_config_ids: list[int] | None = Field(
        None,
        max_length=4,
        min_length=1,
    )


def validate_context(context):
    if context and len(json_format.MessageToJson(context).encode("utf-8")) > 16 * 1024:
        raise Exception(
            f"Context size must not be greater than {16 * 1024} bytes: {len(json_format.MessageToJson(context).encode('utf-8'))}"
        )


def validate_event(event: dict[str, Any]) -> Event:
    if event.get("picture"):
        event["has_picture"] = True
    else:
        event["has_picture"] = False
    return Event.model_validate(event)


def validate_object(object: dict[str, Any]) -> Object:
    pictures = object.get("picture", [])
    picture_labels = [pictures.get("label") for pictures in pictures]
    object["picture_labels"] = picture_labels
    if object.get("data"):
        object["has_extra_data"] = True
    else:
        object["has_extra_data"] = False

    return Object.model_validate(object)


def validate_metrics(metrics: dict[str, Any]) -> list[Metric]:
    metrics_list = []
    for key in metrics.get("metrics", {}):
        m = Metric(
            timestamp=metrics.get("timestamp"),
            units=metrics.get("units"),
            label=key,
            value=metrics.get("metrics", {}).get(key),
            daily_boundary_timezone=metrics.get("daily_boundary_timezone"),
        )
        metrics_list.append(m)
    return metrics_list


def validate_device_status(device_status: dict[str, Any]) -> list[DeviceStatus]:
    device_status_list = []
    for status in device_status.get("device_status", []):
        device_status_list.append(
            DeviceStatus(
                label=status.get("label"),
                status=status.get("status"),
                timestamp=device_status.get("timestamp"),
                score=status.get("score"),
                geometry_config_ids=status.get("geometry_config_ids"),
            )
        )
    return device_status_list
