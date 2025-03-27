import csv
import os
import sys
from pathlib import Path

def format_delimiter(in_file, out_file, old_del, new_del):
    # read
    rows = []
    with open(in_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=old_del, quotechar='"', escapechar='\\')
        for row in reader:
            rows.append(row)

    # write
    with open(out_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=new_del, quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(row)
    return len(rows)

# params
in_dir = '/Users/yyf/Downloads/imdb/'
out_dir = '/Users/yyf/Downloads/imdb-conv/'
old_del = ','
new_del = '|'

csv.field_size_limit(sys.maxsize)
Path(out_dir).mkdir(parents=True, exist_ok=True)
files = os.listdir(in_dir)
for file in files:
    print("Formatting: {} ...\t".format(file), end="", flush=True)
    in_file = in_dir + file
    out_file = out_dir + file
    n_rows = format_delimiter(in_file, out_file, old_del, new_del)
    print("Done ({} rows)".format(n_rows))
