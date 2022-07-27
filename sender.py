import os
import time
import requests
import json
from uuid import uuid4


"http://0.0.0.0:8080/api/"

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def data_prepare(json_data):
    """преобразует входящие словари в список кортежей"""
    queries_in = []
    for d in json_data:
        queries_in += [(d["locale"], d["moduleId"], str(uuid4()),
                        d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]
    return queries_in


# PATH = r"/home/an/Data/Dropbox/data/fast_answers" # Home
PATH = r"/home/alexey/Data/Dropbox/data/fast_answers"  # Office
# file_name = "data_all_with_locale.json"
# file_name = "qa-full-ru.json"
# file_name = "qa-full-ua.json"
file_name = "qa.json"

with open(os.path.join(PATH, file_name), "r") as f:
    initial_data_json = json.load(f)

initial_data = initial_data_json["data"][:10000]

# SERVISE_URL = "http://srv01.lingua.dev.msk2.sl.amedia.tech:8000/api/" # prod
#SERVISE_URL = "http://srv01.lingua.dev.msk2.sl.amedia.tech:7000/api/" # dev
#SERVISE_URL = "http://0.0.0.0:7000/api/"
# SERVISE_URL = "http://127.0.0.1:5000/api/"
#SERVISE_URL = "http://srv01.lingua.dev.msk2.sl.amedia.tech:8080/api/"
SERVISE_URL = "http://0.0.0.0:8080/api/"


do = "add"
#do = "search_one"
#do = "search_a_lot"
#do = "update"
# do = "delete"
#do = "delete_all"

if do == "add":
    splited_data = [x for x in chunks(initial_data, 10000)]
    k = 1
    for chunk in splited_data:
        print(len(chunk))
        sending_data = {"data": chunk, "score": 0.99, "operation": "add"}
        r = requests.post(SERVISE_URL, json=sending_data)
        print(k, "/", len(splited_data))
        k += 1
        print(r)
        print(r.text)
        # print(r.content)

if do == "delete":
    del_data = initial_data # [initial_data[1]]
    print(del_data)
    t = time.time()
    sending_data = {"data": del_data, "score": 0.99, "operation": "delete"}
    r = requests.post(SERVISE_URL, json=sending_data)
    delta = time.time() - t
    print("deleting time:", delta)
    print(r)
    print(r.text)
    print(r.content)

if do == "update":
    del_data = [initial_data[3]]
    print(del_data)
    t = time.time()
    sending_data = {"data": del_data, "score": 0.99, "operation": "update"}
    r = requests.post(SERVISE_URL, json=sending_data)
    delta = time.time() - t
    print("updating time:", delta)

elif do == "search_one":
    searched_data = [initial_data[0]]
    print(searched_data, "\n")
    clusters = []
    for d in searched_data:
        clusters += d["clusters"]
    t = time.time()
    sending_data = {"data": searched_data, "operation": "search", "score": 0.99}
    r = requests.post(SERVISE_URL, json=sending_data)
    delta = time.time() - t
    print(r.json())
    print("clusters quantity:", len(clusters), "searching time:", delta)
    # with open(os.path.join("data", "test.json"), "w") as f:
    #    json.dump(sending_data, f, ensure_ascii=False)

elif do == "search_a_lot":
    secs = 0
    k = 0
    for i in range(25):
        print(k)
        searched_data = [initial_data[i]]
        print(searched_data)
        # print(searched_data, "\n")
        clusters = []
        for d in searched_data:
            clusters += d["clusters"]
        print("clusters quantity:", len(clusters))
        t = time.time()
        sending_data = {"data": searched_data, "operation": "search", "score": 0.99}
        r = requests.post(SERVISE_URL, json=sending_data)
        delta = time.time() - t
        secs += delta
        print(r.json())
        print("clusters quantity:", len(clusters), "searching time:", delta)
        k += 1

    print("time evolution:", secs, secs/(k + 1))
