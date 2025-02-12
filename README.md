## README

This is a Python script written to help categorize Amazon transactions imported into the Monarch Money platform. Currently,
imported bank transactions lack detailed information regarding Amazon purchases making it difficult to categorize
transactions and form budgets. This script takes care of a lot of the manual labor of comparing Amazon Orders via the
website and Monarch Money.

### Current Version 1.0.0

### New in Version 1.0.0
- Now just drag-and-drop your entire Amazon data dump folder onto the script, and it will do the rest. It will automatically find the csvs it needs to process.
- Script defaults to processing the previous month, but you can adjust the start_date and end_date in the config.ini file.
- Fixed bug where the cmd line defaults overroad the config.ini file.
- Added setup.py for easier installation.

### How It Works
The script works by comparing transaction price data from your Amazon orders with bank/credit card transactions imported into Monarch Money (MM),
within a timeframe +/- a few days. The transactions from MM are filtered by merchant name (Amazon/Prime Video), if they have no notes, and category_ids of your choosing.
When a match is found, the script writes the description of the Amazon item in the notes of the transaction for your convenience, and assigns a category for the transaction
using a best guess approach from Claude. 

The script uses a timeframe because transaction post dates are not always the same as the order dates, so a window of time is necessary to compare
transactions. MM transactions are filtered to help with accuracy, and you may use category_ids to further improve accuracy. For example, if you have
MM transaction rules that help identify Amazon transactions by categorizing them with a general category before being further specified (i.e. "unidentified transactions", "needs review").
The script relies on the notes of a transaction being empty to signify that it has not seen the transaction. This is necessary for idempotency if you need to rerun the script.
Additionally, the script does some aggregation work on order ids to calculate the value charged to the bank for proper comparison and matching.

This method is inexact, but generally works. 

Note: The script currently does not differentiate between returns and purchases, and two purchases with the same transaction value around the same time may be mis-assigned.

To get started, you will need:

- To request your Amazon Orders with a [personal information request](https://www.amazon.com/gp/help/customer/display.html?nodeId=TP1zlemejtTn6pwYKS)
- An [API key](https://docs.anthropic.com/en/api/admin-api/apikeys/get-api-key) from Anthropic
- An account with [Monarch Money](https://www.monarchmoney.com/) (you can create a dedicated account for this script if you choose.)

To run, create a config.ini file. The ini should look like this below.

```
[DEFAULT]
api_key = your_anthropic_claude_api_key
email = your_email
password = your_password
sleep_seconds = 1.0
start_date = 2023-01-01
end_date = 2023-01-31
```

1. Drop your data dump folder from the Amazon request directly at the root of this package, it should be called 'Your Orders' by default.
4. Adjust your sleep_seconds to avoid rate limiting.
5. If you want category ids, the script prints all category ids at the beginning before matching and updating. You may exit the script and use that output to filter your transactions further on a subsequent run.
6. Remove any transactions you don't want to process from the csvs.

7. Install the requirements ```pip install -r requirements.txt``` or use your favorite build tool for the setup.py method.

```commandline
python3 ./main.py --config config.ini
```


