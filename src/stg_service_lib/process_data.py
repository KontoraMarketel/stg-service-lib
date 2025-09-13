import json
import pandas as pd


from uuid import UUID, uuid5, NAMESPACE_OID
from datetime import datetime, timezone
from clickhouse_connect.driver.asyncclient import AsyncClient

from custom_types import DataCallback


async def process_data(
    db_conn: AsyncClient,
    data: dict,
    task_id: str,
    ts: str,
    on_data: DataCallback,
) -> None:
    # task_id -> UUID
    try:
        task_uuid = UUID(str(task_id))
    except ValueError:
        task_uuid = uuid5(NAMESPACE_OID, str(task_id))

    # ts -> datetime (UTC)
    if isinstance(ts, (int, float)):
        ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    else:
        ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

    # data["data"] приходит как строка
    raw = data.get("data")
    if not raw:
        return
    if isinstance(raw, str):
        items = json.loads(raw)
    else:
        items = raw

    if not items:
        return

    # Тут мы обрабатываем датафрейм и в конце пишем их в клик
    df = await on_data(pd.DataFrame(items), task_uuid, ts_dt)

    # вставляем DataFrame в ClickHouse
    await db_conn.insert_df(
        "dwh_table", df, settings={"async_insert": 1, "wait_for_async_insert": 0}
    )
