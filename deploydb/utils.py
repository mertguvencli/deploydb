
import os
from datetime import datetime
import csv


def _save_csv(path, columns, rows):
    file_exists = os.path.exists(path)
    mode = 'a' if file_exists else 'w'
    with open(path, mode=mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(columns)
        writer.writerows(rows)


def _set_commit_log(hexsha, path):
    columns = ['hash', 'time']
    row = [[hexsha, datetime.now()]]
    _save_csv(path=path, columns=columns, rows=row)


def _last_commit_hash(path):
    logs = []
    file_exists = os.path.exists(path)
    if file_exists:
        with open(path, 'r') as f:
            logs = [line for line in csv.reader(f)]
    if len(logs) > 1:
        return logs[-1][0]  # commit_id
    return None
