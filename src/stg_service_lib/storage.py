import json

from types_aiobotocore_s3.client import S3Client

from minio_pool import MinioClientPool


async def upload_to_minio(pool: MinioClientPool, bucket, data, key):
    client: S3Client = await pool.acquire()
    try:
        if not isinstance(data, str):
            data = json.dumps(data, ensure_ascii=False)

        await client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data.encode("utf-8"),
            ContentType="application/json"
        )
    except Exception:
        raise
    finally:
        await pool.release(client)


async def download_from_minio(
        pool: MinioClientPool,
        bucket: str,
        key: str,
) -> dict:
    client: S3Client = await pool.acquire()
    try:
        response = await client.get_object(Bucket=bucket, Key=key)
        content = await response['Body'].read()
        return json.loads(content.decode("utf-8"))
    except Exception:
        raise
    finally:
        await pool.release(client)
