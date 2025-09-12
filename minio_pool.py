import aioboto3
import asyncio

from types_aiobotocore_s3 import S3Client


class MinioClientPool:
    def __init__(self, size, endpoint_url, access_key, secret_key):
        self.size = size
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.session = aioboto3.Session()
        self.pool: asyncio.Queue[S3Client] = asyncio.Queue()

    async def start(self):
        for _ in range(self.size):
            client = await self.session.client(
                service_name='s3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key
            ).__aenter__()
            await self.pool.put(client)

    async def stop(self):
        while not self.pool.empty():
            client = await self.pool.get()
            await client.__aexit__(None, None, None)

    async def acquire(self) -> S3Client:
        return await self.pool.get()

    async def release(self, client):
        await self.pool.put(client)
