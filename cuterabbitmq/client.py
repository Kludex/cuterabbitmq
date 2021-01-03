import asyncio
import functools
from typing import Coroutine, Optional, Type, Union

from aio_pika import Message, RobustConnection, connect_robust
from aio_pika.connection import ConnectionType
from aio_pika.exchange import ExchangeType
from aio_pika.message import IncomingMessage
from aio_pika.queue import ConsumerTag, Queue
from aio_pika.types import TimeoutType
from aiormq.types import ConfirmationFrameType


class RabbitMQ:
    def __init__(
        self,
        url: str = None,
        *,
        host: str = "localhost",
        port: int = 5672,
        login: str = "guest",
        password: str = "guest",
        virtualhost: str = "/",
        ssl: bool = False,
        loop: asyncio.AbstractEventLoop = None,
        ssl_options: dict = None,
        timeout: TimeoutType = None,
        connection_class: Type[ConnectionType] = RobustConnection,
        client_properties: dict = None,
        # exchangers: Dict[str, Dict[str, Any]] = None,
        **kwargs,
    ):
        self.url = url
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.virtualhost = virtualhost
        self.ssl = ssl
        self.loop = loop
        self.ssl_options = ssl_options
        self.timeout = timeout
        self.connection_class = connection_class
        self.client_properties = client_properties
        # self.exchangers = exchangers or {}
        self.kwargs = kwargs

    async def connect(self, loop=None):
        self.connection = await connect_robust(
            self.url,
            host=self.host,
            port=self.port,
            login=self.login,
            password=self.password,
            virtualhost=self.virtualhost,
            ssl=self.ssl,
            loop=loop or self.loop,
            ssl_options=self.ssl_options,
            timeout=self.timeout,
            connection_class=self.connection_class,
            client_properties=self.client_properties,
            **self.kwargs,
        )
        return self

    def __await__(self):
        return self.connect().__await__()

    async def include_exchange(
        self,
        name: str,
        *,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        durable: bool = None,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
    ):
        async with self.connection.channel() as channel:
            return await channel.declare_exchange(
                name, type, durable, auto_delete, internal, passive, arguments, timeout
            )

    async def include_queue(
        self,
        name: str = None,
        *,
        durable: bool = None,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
    ):
        async with self.connection.channel() as channel:
            await channel.declare_queue(
                name,
                durable=durable,
                exclusive=exclusive,
                passive=passive,
                auto_delete=auto_delete,
                arguments=arguments,
                timeout=timeout,
            )

    async def publish(
        self,
        message: Message,
        routing_key: str,
        exchange_name: str = "",
        *,
        mandatory: bool = True,
        immediate: bool = False,
        timeout: TimeoutType = None,
    ) -> Optional[ConfirmationFrameType]:
        async with self.connection.channel() as channel:
            if exchange_name:
                exchange = await channel.get_exchange(exchange_name)
                await exchange.publish(
                    message,
                    routing_key,
                    mandatory=mandatory,
                    immediate=immediate,
                    timeout=timeout,
                )
            await channel.default_exchange.publish(
                message,
                routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout,
            )

    def on_consume(
        self,
        name: str,
        *,
        no_ack: bool = False,
        exclusive: bool = False,
        consumer_tag: ConsumerTag = None,
        timeout: TimeoutType = None,
    ):
        def consume_handler(function: Coroutine):
            async def wrapper(*args, **kwargs):
                partial = functools.partial(function, *args, **kwargs)
                async with self.connection.channel() as channel:
                    queue = await channel.get_queue(name)
                    await queue.consume(
                        partial,
                        no_ack=no_ack,
                        exclusive=exclusive,
                        consumer_tag=consumer_tag,
                        timeout=timeout,
                    )

            return wrapper

        return consume_handler

    async def bind(self, queue: str, exchange: str, routing_key: str = None):
        async with self.connection.channel() as channel:
            queue = await channel.get_queue(queue)
            exchange = await channel.get_exchange(exchange)
            await queue.bind(exchange=exchange, routing_key=routing_key)


rabbit = RabbitMQ(host="localhost", port=5672)


async def main():
    connection = await rabbit.connect()
    await connection.include_exchange("logs")
    await connection.publish(
        Message(body=b"Hello World!"), routing_key="info", exchange_name="logs"
    )
    print("Sent!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
