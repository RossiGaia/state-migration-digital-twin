import pandas
import datetime

source_log_file_path = "./dt_main_DT1_ABCDEF_s.log"
target_log_file_path = "./dt_main_DT1_ABCDEF_d.log"

source_log_list = []
target_log_list = []
with open(source_log_file_path) as file:
    for line in file.readlines():
        source_log_list.append(line.strip())

with open(target_log_file_path) as file:
    for line in file.readlines():
        target_log_list.append(line.strip())

source_log = "\n".join(source_log_list)
target_log = "\n".join(target_log_list)

print("-----------------------SOURCE-----------------------")
print(source_log)
print("----------------------------------------------------")
print("-----------------------TARGET-----------------------")
print(target_log)
print("----------------------------------------------------")

migrations_start = []
migrations_end = []
migration_duration = []
downtimes_start = []
downtimes_end = []

for line in source_log_list:
    if "Disconnecting from mqtt" in line:
        downtimes_start.append(line.split(": ")[-1])

previously_restored = False
for line in target_log_list:
    if "Restoring total time" in line:
        splitted_line = line.split(": ")
        migrations_start.append(splitted_line[2].split(", ")[0])
        previously_restored = True
        continue
    if "Processing started" in line and previously_restored:
        splitted_line = line.split(": ")
        downtimes_end.append(splitted_line[1])
        migrations_end.append(splitted_line[1])
        previously_restored = False

data = pandas.DataFrame(
    {
        "migration_start": migrations_start,
        "migration_end": migrations_end,
        "downtime_start": downtimes_start,
        "downtime_end": downtimes_end,
    }
)

cols = ["migration_start", "migration_end", "downtime_start", "downtime_end"]
data[cols] = data[cols].astype(float)

data["migration_duration_s"] = data["migration_end"] - data["migration_start"]
data["downtime_duration_s"] = data["downtime_end"] - data["downtime_start"]

print(data)
