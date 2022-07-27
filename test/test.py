import random
import sys
import subprocess
import string

map = {}

def check_query_db(database_path, key, value, found):
    print("checking with " + str(key) + " " + str(value) + " " + str(found))
    assert len(key) > 0 and len(key) <= 100
    output = subprocess.check_output(["./kvdb", "get", database_path, key])
    if found:
        return output == str("value: " + value + "\n")
    else:
        return output == str("element not found.\n")


def query_db_and_check(database_path):
    if len(map) == 0:
        return
    key = random.choice(list(map.keys()))
    check_query_db(database_path, key, map[key], True)

def insert_into_map_and_check(database_path, key_length, key_prefix_char, value_length):
    assert key_length > 0 and key_length <=100
    assert len(key_prefix_char) == 1
    key = key_prefix_char + ''.join(random.choices(string.ascii_letters + string.digits, k=key_length-1))
    value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_length))
    map[key] = value
    output = subprocess.check_output(["./kvdb", "set", database_path, key, value])
    assert output == b"successfully inserted element.\n"
    check_query_db(database_path, key, value, True)

def delete_from_map_and_check(database_path):
    if len(map) == 0:
        return
    key = random.choice(list(map.keys()))
    map.pop(key)
    output = subprocess.check_output(["./kvdb", "del", database_path, key])
    assert output == b"successfully deleted element.\n"
    check_query_db(database_path, key, "", False)

def replace_from_map_and_check(database_path, value_length):
    if len(map) == 0:
        return
    key = random.choice(list(map.keys()))
    value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_length))
    map[key] = value
    output = subprocess.check_output(["./kvdb", "set", database_path, key, value])
    assert output == b"successfully inserted element.\n"
    check_query_db(database_path, key, value, True)


def main(argv):
    assert len(argv) == 6
    database_path = argv[1];
    key_length = int(argv[2])
    value_length = int(argv[3])
    prefix = argv[4]
    number_of_tests = int(argv[5])

    try:
        for i in range(number_of_tests):
            operation = random.choices([0, 1, 2, 3], weights=[3, 1, 1, 1])[0]
            if operation == 0:
                insert_into_map_and_check(database_path, key_length, prefix, value_length)
            elif operation == 1:
                delete_from_map_and_check(database_path)
            elif operation == 2:
                replace_from_map_and_check(database_path, value_length)
            elif operation == 3:
                query_db_and_check(database_path)

    except:
        print(map)
    


if __name__ == "__main__":
    main(sys.argv);
