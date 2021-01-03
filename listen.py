import asyncio

from aio_pika.message import IncomingMessage

from cuterabbitmq.client import RabbitMQ

rabbit = RabbitMQ(host="localhost", port=5672)


async def main(loop):
    connection = await rabbit.connect(loop)
    await connection.include_exchange("logs")
    await connection.include_queue("potato", durable=True)
    await connection.bind(queue="potato", exchange="logs", routing_key="info")
    await connection.bind(queue="potato", exchange="logs", routing_key="warning")

    @rabbit.on_consume("potato")
    async def consume_logs(message: IncomingMessage):
        print("Received!")
        print(message.channel)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()
