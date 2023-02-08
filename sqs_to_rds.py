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

store = Ssm()

plays_sql = "insert into play_orchestrators (play_id, run_type, start_time_local, start_time_utc) VALUES (%s, %s, %s, %s)"
instances_sql = "insert into instance_results (instance_id, average_buy_price, average_sell_price, bought_value, buy_order_count, play_config_name, run_id, sell_order_count, sell_order_filled_count, sold_value, symbol, symbol_group, total_gain, units, weather_condition) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"

sqs_client = boto3.client("sqs")
sqs_queue_url = store.get("/tabot/telemetry/queue/backtest")


# need to add a unique ID per instance
# need to add the datetime that the instance started and ended
async def insert_instance_terminated(
    mycursor,
    average_buy_price,
    average_sell_price,
    bought_value,
    buy_order_count,
    instance_id,
    play_config_name,
    run_id,
    sell_order_count,
    sell_order_filled_count,
    sold_value,
    symbol,
    symbol_group,
    total_gain,
    units,
    weather_condition,
    **kwargs,
):
    vals = (
        instance_id,
        average_buy_price,
        average_sell_price,
        bought_value,
        buy_order_count,
        play_config_name,
        run_id,
        sell_order_count,
        sell_order_filled_count,
        sold_value,
        symbol,
        symbol_group,
        total_gain,
        units,
        weather_condition,
    )
    sql = "insert into instance_results (instance_id, average_buy_price, average_sell_price, bought_value, buy_order_count, play_config_name, run_id, sell_order_count, sell_order_filled_count, sold_value, symbol, symbol_group, total_gain, units, weather_condition) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"
    try:
        mycursor.execute(sql, vals)
    except mysql.connector.IntegrityError as e:
        if "Duplicate entry" not in e.msg:
            raise
    else:
        print(f"New instance result: \t\t{run_id} instance {instance_id}")


async def insert_play_orchestrator(
    mycursor, play_id, run_type, start_time_local, start_time_utc, **kwargs
):
    vals = (play_id, run_type, start_time_local, start_time_utc)
    sql = "insert into play_orchestrators (play_id, run_type, start_time_local, start_time_utc) VALUES (%s, %s, %s, %s)"
    try:
        mycursor.execute(sql, vals)
    except mysql.connector.IntegrityError as e:
        if "Duplicate entry" not in e.msg:
            raise
    else:
        print(f"New Play Orchestrator: \t\t{play_id}")


async def do_insert(mycursor, values_list, sql_string):
    try:
        mycursor.executemany(sql_string, values_list)
    except mysql.connector.IntegrityError as e:
        if "Duplicate entry" not in e.msg:
            raise
        return True
    else:
        return True


async def makeitem() -> str:
    messages = sqs_client.receive_message(
        QueueUrl=sqs_queue_url, MaxNumberOfMessages=10
    )
    if "Messages" in messages.keys():
        return messages
    else:
        await asyncio.sleep(10)


async def produce(name: int, q: asyncio.Queue) -> None:
    #    n = random.randint(0, 10)
    #    for _ in it.repeat(None, n):  # Synchronous loop for each single producer
    # await randsleep(caller=f"Producer {name}")
    while True:
        i = await makeitem()
        t = time.perf_counter()
        if i:
            await q.put((i, t))
            print(f"Producer {name} added {len(i['Messages'])} to queue.")


async def consume(name: int, q: asyncio.Queue) -> None:
    print("** {name} CONNECTED TO SQL **")
    _base = "/tabot/telemetry/sql/"
    mydb = mysql.connector.connect(
        host=store.get(f"{_base}host"),
        user=store.get(f"{_base}user"),
        password=store.get(f"{_base}password"),
        database=store.get(f"{_base}database"),
    )
    mycursor = mydb.cursor()
    while True:
        # await randsleep(caller=f"Consumer {name}")
        bulk_plays = []
        bulk_instances = []
        i, t = await q.get()

        for m in i["Messages"]:
            body = m["Body"]
            body_json = json.loads(body)

            event = body_json["event"].lower()
            if event == "play start":
                bulk_plays.append(
                    (
                        body_json["play_id"],
                        body_json["run_type"],
                        body_json["start_time_local"],
                        body_json["start_time_utc"],
                    )
                )

                # insert_play_orchestrator(**body_json)
            elif event == "instance terminated":
                bulk_instances.append(
                    (
                        body_json["instance_id"],
                        body_json["average_buy_price"],
                        body_json["average_sell_price"],
                        body_json["bought_value"],
                        body_json["buy_order_count"],
                        body_json["play_config_name"],
                        body_json["run_id"],
                        body_json["sell_order_count"],
                        body_json["sell_order_filled_count"],
                        body_json["sold_value"],
                        body_json["symbol"],
                        body_json["symbol_group"],
                        body_json["total_gain"],
                        body_json["units"],
                        body_json["weather_condition"],
                    )
                )
                # insert_instance_terminated(**body_json)

        if bulk_plays:
            if not await do_insert(mycursor, bulk_plays, plays_sql):
                print("Insert failure!")
            print(f"{len(bulk_plays)} plays inserted")
        if bulk_instances:
            if not await do_insert(mycursor, bulk_instances, instances_sql):
                print("Insert failure!")
            # print(f"{len(bulk_instances)} instances inserted")

        sqs_client.delete_message_batch(
            QueueUrl=sqs_queue_url,
            Entries=[
                {"Id": d["MessageId"], "ReceiptHandle": d["ReceiptHandle"]}
                for d in i["Messages"]
            ],
        )

        now = time.perf_counter()
        print(f"Consumer {name} committed {len(i['Messages'])} {now-t:0.5f} seconds.")
        q.task_done()


async def main(nprod: int, ncon: int):
    q = asyncio.Queue()
    producers = [asyncio.create_task(produce(n, q)) for n in range(nprod)]
    consumers = [asyncio.create_task(consume(n, q)) for n in range(ncon)]

    # for p in producers:
    #    asyncio.create_task(counter('slow', 1))

    await asyncio.Event().wait()

    # await asyncio.gather(*producers)
    # await q.join()  # Implicitly awaits consumers, too
    # for c in consumers:
    #    c.cancel()


if __name__ == "__main__":
    random.seed(444)
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--nprod", type=int, default=5)
    parser.add_argument("-c", "--ncon", type=int, default=3)
    ns = parser.parse_args()
    start = time.perf_counter()

    asyncio.run(main(**ns.__dict__))
    elapsed = time.perf_counter() - start
    print(f"Program completed in {elapsed:0.5f} seconds.")
