import asyncio
import itertools as it
import os
import random
import time
import boto3
import time
import json
import mysql.connector
from parameter_store import Ssm
import argparse


class CommitSizeReachedError(Exception):
    ...


TIMEOUT = 10
COMMIT_SIZE = 100
store = Ssm()
sqs_client = boto3.client("sqs")
sqs_queue_url = store.get("/tabot/telemetry/queue/backtest")

sqs = boto3.resource("sqs")
queue = sqs.Queue(store.get("/tabot/telemetry/queue/backtest"))


async def main():
    messages = []

    # keep trying this forever
    while True:
        try:
            z = asyncio.wait_for(get_sqs_messages(), TIMEOUT)
            sqs_messages = await z
            try:
                new_messages = sqs_messages["Messages"]
            except TypeError:
                ...
                print("type error")
            except Exception as e:
                print(f"Something went wrong: {str(e)}")
            else:
                messages.append(new_messages)

                if len(messages) == COMMIT_SIZE:
                    raise CommitSizeReachedError
                print(f"Len of new_messages: {len(messages)}")
        except (TimeoutError, CommitSizeReachedError):
            # no more messages
            print("do something with messages")


async def get_sqs_messages():
    # messages = queue.receive_messages(MaxNumberOfMessages=10)
    messages = sqs_client.receive_message(
        QueueUrl=sqs_queue_url, MaxNumberOfMessages=10
    )

    if "Messages" in messages.keys():
        return messages
    # will return None if the AWS endpoint returns no messages

    # else:
    #    print("this should not happen?")
    #    # this should never happen since receive_message is


if __name__ == "__main__":
    asyncio.run(main())
