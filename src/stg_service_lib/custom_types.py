import pandas as pd

from uuid import UUID
from datetime import datetime
from typing import Callable, TypedDict, Optional, Awaitable


type DataCallback = Callable[[pd.DataFrame, UUID, datetime], Awaitable[pd.DataFrame]]


class KafkaMessage(TypedDict):
    task_id: str
    minio_key: str
    ts: datetime


class ProcessingResult(TypedDict):
    task_id: str
    ts: datetime
    success: bool
    error: Optional[str]
