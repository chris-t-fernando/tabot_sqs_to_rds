import boto3
import time
import json
import mysql.connector
from parameter_store import Ssm

from multiprocessing import Process

# TODO this is so unnecessarily complex. Could just consolidate it down to one
# pass per read, but whatever


def do_insert(mycursor, values_list, sql_string):
    try:
        mycursor.executemany(sql_string, values_list)
    except mysql.connector.IntegrityError as e:
        if "Duplicate entry" not in e.msg:
            raise
        return True
    else:
        return True


def process_queue():
    store = Ssm()

    sqs_client = boto3.client("sqs")
    sqs_queue_url = store.get("/tabot/telemetry/queue/backtest")

    _base = "/tabot/telemetry/sql/"
    mydb = mysql.connector.connect(
        host=store.get(f"{_base}host"),
        user=store.get(f"{_base}user"),
        password=store.get(f"{_base}password"),
        database=store.get(f"{_base}database"),
    )
    mycursor = mydb.cursor()

    # mycursor.execute(
    #    "CREATE TABLE play_orchestrators (play_id VARCHAR(30) PRIMARY KEY, run_type VARCHAR(255), start_time_local DATETIME, start_time_utc DATETIME)"
    # )

    plays_sql = "insert into play_orchestrators (play_id, run_type, start_time_local, start_time_utc) VALUES (%s, %s, %s, %s)"
    instances_sql = "insert into instance_results (instance_id, average_buy_price, average_sell_price, bought_value, buy_order_count, play_config_name, run_id, sell_order_count, sell_order_filled_count, sold_value, symbol, symbol_group, total_gain, units, weather_condition) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"

    bulk_plays = []
    bulk_instances = []
    bulk_handles = []
    sqs = boto3.resource("sqs")

    # instances = []
    # plays = []
    # handles = []
    queue = sqs.Queue(store.get("/tabot/telemetry/queue/backtest"))

    while True:
        last = time.time()
        messages = sqs_client.receive_message(
            QueueUrl=sqs_queue_url, MaxNumberOfMessages=10
        )
        # print(f"SQS recv {time.time() - last} seconds")
        last = time.time()

        try:
            messages["Messages"]
        except KeyError:
            # nothing to do, so do the inserts and commit, and then sleep for 30 seconds
            # print("nothing to do, so doing commmit")
            if bulk_plays:
                if not do_insert(mycursor, bulk_plays, plays_sql):
                    print("Insert failure!")
                # print(f"Plays insert for {time.time() - last} seconds")
                last = time.time()
            if bulk_instances:
                if not do_insert(mycursor, bulk_instances, instances_sql):
                    print("Insert failure!")
                # print(f"Instances insert for {time.time() - last} seconds")
                last = time.time()

            if bulk_handles:
                while len(bulk_handles) > 0:
                    trim = 10 if len(bulk_handles) > 10 else len(bulk_handles)
                    these_handles = bulk_handles[:10]
                    sqs_client.delete_message_batch(
                        QueueUrl=sqs_queue_url,
                        Entries=these_handles,
                    )
                    # print(f"SQS clear ran for {time.time() - last} seconds")
                    bulk_handles = bulk_handles[10:]

                last = time.time()
                mydb.commit()
                # print(f"Commit ran for {time.time() - last} seconds")

            print(
                f"Committed {len(bulk_plays)} plays and {len(bulk_instances)} instances"
            )
            bulk_handles = []
            bulk_instances = []
            bulk_plays = []

            time.sleep(1)

        else:
            # plays = []
            # instances = []

            play_count = 0
            instance_count = 0
            for m in messages["Messages"]:
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
                    play_count += 1

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
                    instance_count += 1
                    # insert_instance_terminated(**body_json)

            bulk_handles += [
                {"Id": d["MessageId"], "ReceiptHandle": d["ReceiptHandle"]}
                for d in messages["Messages"]
            ]
            # print(
            #    f"Queued {len(messages['Messages'])} messages ({play_count} plays and {instance_count} instances)"
            # )

        # time.sleep(2)


if __name__ == "__main__":  # confirms that the code is under main function
    procs = []
    for z in range(20):
        proc = Process(target=process_queue)
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()
