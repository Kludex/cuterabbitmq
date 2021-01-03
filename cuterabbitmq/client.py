import asyncio
from typing import Optional, Type, Union

from aio_pika import Message, RobustConnection, connect_robust
from aio_pika.connection import ConnectionType
from aio_pika.exchange import ExchangeType
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

    async def connect(self):
        self.connection = await connect_robust(
            self.url,
            host=self.host,
            port=self.port,
            login=self.login,
            password=self.password,
            virtualhost=self.virtualhost,
            ssl=self.ssl,
            loop=self.loop,
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
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        durable: bool = None,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
    ):
        async with self.connection.channel() as channel:
            await channel.declare_exchange(
                name, type, durable, auto_delete, internal, passive, arguments, timeout
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
            return await channel.default_exchange.publish(
                message,
                routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout,
            )

    # def on_consume(self, no_ack: bool):
    #     def consume_handler(function: Callable):
    #         def wrapper(channel, method_frame, header_frame, body):
    #             ...

    #         return wrapper

    #     return consume_handler


async def main():
    rabbit = await RabbitMQ(host="localhost", port=5672)
    await rabbit.include_exchange("logs")
    await rabbit.publish(
        Message(body=b"Hello World!"), routing_key="info", exchange_name="logs"
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
