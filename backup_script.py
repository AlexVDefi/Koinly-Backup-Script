import os, shutil
from os.path import dirname as up
import pandas as pd
import numpy as np
import re
from time import sleep

parent_dir = os.path.abspath(up(__file__))

csv_name = input("Write the exact name of the csv including the .csv extension. Like csv1.csv for example."
                 "The file needs to be in the same folder as the script and it's case sensitive:\n")
csv_name = os.path.join(parent_dir, csv_name)
pd.options.mode.chained_assignment = None

backup_dir = os.path.join(parent_dir, r'backup\\')
for filename in os.listdir(backup_dir):
    file_path = os.path.join(backup_dir, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    except Exception as e:
        print('Failed to delete %s. Reason: %s' % (file_path, e))

currency = input("What is the base currency of the account? Use fiat symbol ie. USD, AUD, EUR etc.\n").upper()
print("Generating files...")
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', None)
data = pd.read_csv(csv_name, skiprows=2)
data['Label'] = data['Label'].replace(to_replace="staking", value="reward")
data['Label'] = data['Label'].replace(to_replace="other_income", value="income")
data.drop([f"Gain ({currency})", f"Net Value ({currency})", "Type", f"Fee Value ({currency})", "TxSrc", "TxDest", "Received Cost Basis", "Sent Cost Basis"], inplace=True, axis=1)
send_grouped_data = data.groupby("Sending Wallet")
received_grouped_data = data.groupby("Receiving Wallet")
for data in send_grouped_data:
    name = data[0]
    data = data[1]
    exchanges = data[data['Receiving Wallet'] == name]
    exchanges = exchanges[exchanges['Label'] != "to_pool"]
    exchanges = exchanges[exchanges['Label'] != "from_pool"]
    to_pool = data[(data['Label'] == "to_pool") & (data['Receiving Wallet'] == name)]
    to_pool['Label'] = to_pool['Label'].replace(to_replace="to_pool", value=np.nan)
    to_pool['Received Amount'] = to_pool['Received Amount'].apply(lambda x : x if x < 0 else np.nan)
    to_pool['Received Currency'] = to_pool['Received Currency'].replace(to_replace=r"(.*)", value=np.nan, regex=True)
    to_pool['Description'] = "To Pool"
    query = data[data['Receiving Wallet'] != name]
    query['Received Amount'] = query['Received Amount'].apply(lambda x : x if x < 0 else np.nan)
    query['Received Currency'] = query['Received Currency'].replace(to_replace=r"(.*)", value=np.nan, regex=True)
    data = pd.concat([query, exchanges, to_pool], axis=0)
    data.drop(["Receiving Wallet", "Sending Wallet"], inplace=True, axis=1)
    invalid_name = re.findall(r'([<>:"/\\|?*])', name)
    if invalid_name:
        for i in invalid_name:
            name = name.replace(i, "")
    output_path = f"{name}.csv"
    data.to_csv(backup_dir+output_path, mode="a", header=not os.path.exists(output_path), index=False)
for data in received_grouped_data:
    name = data[0]
    data = data[1]
    from_pool = data[(data['Label'] == "from_pool") & (data['Receiving Wallet'] == name)]
    from_pool['Label'] = from_pool['Label'].replace(to_replace="from_pool", value=np.nan)
    from_pool['Sent Amount'] = from_pool['Sent Amount'].apply(lambda x : x if x < 0 else np.nan)
    from_pool['Sent Currency'] = from_pool['Sent Currency'].replace(to_replace=r"(.*)", value=np.nan, regex=True)
    from_pool['Description'] = "From Pool"
    query = data[data['Sending Wallet'] != name]
    query['Sent Amount'] = query['Sent Amount'].apply(lambda x : x if x < 0 else np.nan)
    query['Sent Currency'] = query['Sent Currency'].replace(to_replace=r"(.*)", value=np.nan, regex=True)
    data = pd.concat([query, from_pool], axis=0)
    data.drop(["Receiving Wallet", "Sending Wallet"], inplace=True, axis=1)
    invalid_name = re.findall(r'([<>:"/\\|?*])', name)
    if invalid_name:
        for i in invalid_name:
            name = name.replace(i, "")
    output_path = f"{name}.csv"
    data.to_csv(backup_dir+output_path, mode="a", header=not os.path.exists(output_path), index=False)

print("Done! Files are saved in the backup folder.")
sleep(5)