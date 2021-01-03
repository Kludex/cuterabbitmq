import asyncio
from typing import Callable, List, Type

from aio_pika import Message, RobustConnection, connect_robust
from aio_pika.connection import ConnectionType

# from aio_pika.robust_exchange import RobustExchange
from aio_pika.types import TimeoutType

# class Consumer:
#     ...


# class Publisher:
#     ...


class RabbitMQClient:
    # consumers: List[Consumer]
    # publishers: List[Publisher]

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
        **kwargs
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
            **self.kwargs
        )
        return self.connection

    def __await__(self):
        return self.connect().__await__()

    # def on_publish(self, exchange: RobustExchange):
    #     def publish_handler(function: Callable):
    #         ...

    #     return publish_handler

    # def on_consume(self, no_ack: bool):
    #     def consume_handler(function: Callable):
    #         def wrapper(channel, method_frame, header_frame, body):
    #             ...

    #         return wrapper

    #     return consume_handler


async def main():
    rabbit = await RabbitMQClient(host="localhost", port=5672)
    print(rabbit)

    # @rabbit.on_consume()
    # def consume_test():
    #     print("hi")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
