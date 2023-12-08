""" SETUP
- <https://www.rabbitmq.com/download.html>
- we can run it as a docker: `docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management`
- and then access from <http://localhost:15672/#/>
"""

from datetime import datetime
from rococo.messaging import RabbitMqConnection
import asyncio

QUEUE_NAME: str = 'rbmq-queue-er-sir'

current_time = datetime.now()
formatted_time = current_time.strftime("%H:%M:%S")
msg = {'messageN1': f'data:{formatted_time}'}

def process_message(message_data: dict):
    print(f"Processing message {message_data}...")


# with RabbitMqConnection(host='localhost', port=5672, username='guest', password='guest', virtual_host='/') as conn:
#     # Producer
#     conn.send_message(QUEUE_NAME, msg)
#     print(f"sent {msg}")
#     # Consumer
#     conn.consume_messages(QUEUE_NAME, process_message)

# print("after sending")



async def consume_messages_async(conn, queue_name, callback_function):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, conn.consume_messages, queue_name, callback_function)

async def main():
    with RabbitMqConnection(host='localhost', port=5672, username='guest', password='guest', virtual_host='/') as conn:
        # Producer
        conn.send_message(QUEUE_NAME, msg)
        print(f"sent {msg}")

        # Start consuming messages asynchronously in the background
        task = asyncio.create_task(consume_messages_async(conn, QUEUE_NAME, process_message))

        # Continue with the main code
        print("after task creation")

        # Wait for the message consumption task to complete
        await task

        # Continue with the main code
        print("after `await task`")

# Run the main function asynchronously
asyncio.run(main())
