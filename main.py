import asyncio
import csv
import time
import argparse
import numpy as np
import pandas as pd

import anthropic
from anthropic import AsyncAnthropic

from datetime import datetime, timedelta
from typing import List, Tuple
from collections import defaultdict
from pprint import pprint
import configparser
from monarchmoney import MonarchMoney

DEFAULT_RECORD_LIMIT = 100
ORDER_ID_INDEX = 0
SUB_TOTAL_INDEX = 4
DESCRIPTION_INDEX = 1
DELIVERY_DATE_INDEX = 2
ORDER_DATE_INDEX = 3
IS_DIGITAL_ORDER_INDEX = 5

__version__ = "1.0.0"

def process_csv_files(orders_file: str, transactions_file: str) -> []:
    # Read the CSV files
    orders_df = pd.read_csv(orders_file)

    transactions_df = pd.read_csv(transactions_file)

    # count the number of NAs in the column.
    print(transactions_df[pd.to_numeric(transactions_df['TransactionAmount'], errors='coerce').isna()][
              'TransactionAmount'])
    transactions_df['TransactionAmount'] = (transactions_df['TransactionAmount']
                                            .replace('Not Applicable', np.nan)
                                            .astype(float))

    # Group transactions by DigitalOrderItemId and sum TransactionAmount
    transactions_grouped = transactions_df.groupby('DigitalOrderItemId')['TransactionAmount'].sum().reset_index()

    # Merge the dataframes
    merged_df = orders_df.merge(
        transactions_grouped,
        on='DigitalOrderItemId',
        how='left'
    )

    # Create the final dictionary
    result = []
    for _, row in merged_df.iterrows():
        result.append([row['DigitalOrderItemId'], row['ProductName'], row['FulfilledDate'], row['OrderDate'],
                       row['TransactionAmount'], 1
                       ])

    return result


async def classify_item(anthropic_client: any, categories: List[str], description: str):
    prompt = f"Given the following categories:<categories>{', '.join(categories)}</categories>, classify the item with description <description>{description}</description>. Respond with only the category, no other text."
    prompt.encode('utf-8')

    try:
        response = await anthropic_client.messages.create(
            messages=[{'role': 'user', 'content': prompt}],
            stop_sequences=[anthropic.HUMAN_PROMPT],
            max_tokens=1000,
            model='claude-3-5-sonnet-20240620'
        )

        pprint(response.content)
        result = response.content[0].text.strip()

        if result in categories:
            return result
        else:
            return "No matching category found"

    except anthropic.APIConnectionError as e:
        print("The server could not be reached")
        print(e)  # an underlying Exception, likely raised within httpx.


def process_categories(category_dict):
    # todo type hint this
    category_names = []
    category_id_map = {}

    for category in category_dict['categories']:
        name = category['name']
        category_id = category['id']

        category_names.append(name)
        category_id_map[name] = category_id

    return category_names, category_id_map


def parse_data(csv_file, digital_item_csv, digital_transaction_csv, start_date: str, end_date: str):
    # todo type hint this
    retail_order_id_index = 1
    retail_sub_total_index = 9
    retail_description_index = 23
    retail_delivery_date_index = 18
    retail_order_date_index = 2

    def str_to_float(string, is_digital_order):
        if not is_digital_order:
            # Remove commas from the string
            string = string.replace(',', '')

        # Convert the modified string to a float
        return round(float(string), 2)

    def clean_retail_data(csv_file):
        with open(csv_file, 'r') as file:
            retail_data = csv.reader(file)
            next(retail_data)  # Skip the header row
            cleaned_data = []
            for retail_row in retail_data:
                cleaned_data.append(
                    [retail_row[retail_order_id_index], retail_row[retail_description_index],
                     retail_row[retail_delivery_date_index],
                     retail_row[retail_order_date_index], retail_row[retail_sub_total_index], 0])
            return cleaned_data

    orders = defaultdict(dict)
    try:
        cleaned_retail_data = clean_retail_data(csv_file)
    except FileNotFoundError as e:
        print(f"Error processing CSV file: {e}")
        cleaned_retail_data = []

    try:
        digital_data = process_csv_files(digital_item_csv, digital_transaction_csv)
    except FileNotFoundError as e:
        print(f"Error processing CSV files: {e}")
        digital_data = []

    data = cleaned_retail_data + digital_data
    data.sort(key=lambda x: x[ORDER_DATE_INDEX])

    for row in data:
        if datetime.strptime(start_date, "%Y-%m-%d") <= datetime.strptime(row[ORDER_DATE_INDEX].split('T')[0], "%Y-%m-%d") <= datetime.strptime(end_date, "%Y-%m-%d"):
            # if not is_digital_order_data_set and row[16] == 'Cancelled':
            #     continue
            order_id = f'{row[ORDER_ID_INDEX]}'
            item_subtotal = str_to_float(row[SUB_TOTAL_INDEX], row[IS_DIGITAL_ORDER_INDEX])
            if order_id in orders:
                orders[order_id]['total_cost'] += item_subtotal
                orders[order_id]['description'] += ' ' + row[DESCRIPTION_INDEX]
            else:
                orders[order_id] = {'total_cost': item_subtotal, 'description': row[DESCRIPTION_INDEX],
                                    'delivery_date': row[DELIVERY_DATE_INDEX], 'order_date': row[ORDER_DATE_INDEX]}
        else:
            print(f"Skipping row order: {row[ORDER_ID_INDEX]} as it is outside the date range.")
    return orders


async def match_and_update_transactions(mm: MonarchMoney, anthropic_client: any, csv_file: str,
                                        digital_items_csv_file: str,
                                        digital_transact_csv: str,
                                        category_ids: List[str], sleep_seconds: float, start_date: str,
                                        end_date: str) -> None:
    unmatched_rows: List[Tuple[str, str, float]] = []
    mm_categories = await mm.get_transaction_categories()
    cat_names, cat_map = process_categories(mm_categories)
    pprint("category list")
    pprint(cat_map)

    orders = parse_data(csv_file, digital_items_csv_file, digital_transact_csv, start_date, end_date)
    for order_id, items in orders.items():

        pprint(f' processing orderID: {order_id}')
        if items['delivery_date'] == 'Not Available':
            continue
        try:
            transaction_date_start = (datetime.strptime(items['order_date'], '%Y-%m-%dT%H:%M:%SZ') - timedelta(
                days=1)).strftime('%Y-%m-%d')

        except ValueError as e:
            # pprint(e)
            transaction_date_start = (datetime.strptime(items['order_date'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(
                days=1)).strftime('%Y-%m-%d')

        try:
            transaction_date_end = (
                    datetime.strptime(items['delivery_date'], '%Y-%m-%dT%H:%M:%SZ') + timedelta(days=4)).strftime(
                '%Y-%m-%d')
        except ValueError as e:
            # pprint(e)
            try:
                transaction_date_end = (
                        datetime.strptime(items['delivery_date'], '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(
                    days=4)).strftime('%Y-%m-%d')
            except ValueError as e:
                # pprint(e)
                transaction_date_end = transaction_date_start

        total_cost = round(items['total_cost'], 2)

        offset = 0

        while True:
            matched = False
            amazon_transactions = await mm.get_transactions(
                limit=DEFAULT_RECORD_LIMIT,
                offset=offset,
                start_date=transaction_date_start,
                end_date=transaction_date_end,
                category_ids=category_ids,
                search='Amazon',
                has_notes=False
            )
            prime_transactions = await mm.get_transactions(
                limit=DEFAULT_RECORD_LIMIT,
                offset=offset,
                start_date=transaction_date_start,
                end_date=transaction_date_end,
                category_ids=category_ids,
                search='Prime Video',
                has_notes=False
            )
            transactions = amazon_transactions['allTransactions']['results'] + prime_transactions['allTransactions'][
                'results']

            # pprint(f'Checked for transactions from {transaction_date_start} to {transaction_date_end}')
            pprint(f'transactions found for comparison: {len(transactions)}')
            for transaction in transactions:
                rounded_transaction = round(abs(transaction['amount']), 2)
                # print(f'transaction amount un-rounded: ${transaction['amount']}, rounded: ${rounded_transaction}, '
                #       f'total_cost: ${total_cost}')
                if rounded_transaction == total_cost:
                    matched = True
                    predicted_category = await classify_item(anthropic_client, cat_names, items['description'])
                    pprint(f"Matched Item Description: {items['description']}")
                    pprint(f"Matched Predicted category: {predicted_category}")
                    await mm.update_transaction(
                        transaction_id=transaction['id'],
                        notes=items['description'] + ' ~Automatically applied via auto-classifier script~',
                        category_id=cat_map.get(predicted_category, None)
                    )
                    time.sleep(sleep_seconds)
                    break

            if matched or len(transactions) < DEFAULT_RECORD_LIMIT:
                break

            offset += DEFAULT_RECORD_LIMIT
            time.sleep(sleep_seconds)

        if not matched:
            unmatched_rows.append((items['order_date'], items['delivery_date'], items['description'], total_cost))

    if unmatched_rows:
        print("Unmatched rows, or rows that were already matched:")
        for row in unmatched_rows:
            print(
                f"Order Date: {row[ORDER_ID_INDEX]}, Delivery Date: {row[DELIVERY_DATE_INDEX]}, Description: {row[DESCRIPTION_INDEX]}, Total Cost: ${row[3]}")


def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    # Convert config section to dict, stripping quotes if present
    return {k: v.strip("'\"") for k, v in config['DEFAULT'].items()}


def get_first_of_previous_month():
    ## return first of previous month in ISO
    return (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).date().isoformat()


def get_last_of_previous_month():
    ## return last of previous month in ISO
    return (datetime.now().replace(day=1) - timedelta(days=1)).date().isoformat()


def parse_args():
    parser = argparse.ArgumentParser(description='Process command-line arguments.')
    parser.add_argument('--config', help='Config file path for arguments')

    parser.add_argument('--category_ids', nargs='+', required=False,
                        help='Category IDs to filter (space-separated)')
    parser.add_argument('--api_key', required=False,  # Changed to False since it might come from config
                        help='Anthropic API key')
    parser.add_argument('--email', required=False,
                        help='Monarch email')
    parser.add_argument('--password', required=False,
                        help='Monarch password')
    parser.add_argument('--sleep_seconds', default=1.0,
                        help='Sleep seconds to mitigate rate limits by Anthropic')
    parser.add_argument('--is_digital_order',
                        help='Set to True if processing a digital order')
    parser.add_argument('--start_date', default=get_first_of_previous_month(), required=False,
                        help="The earliest date from which you'd like to process transactions. Defaults to the first of last month.")
    parser.add_argument('--end_date', default=get_last_of_previous_month(), required=False,
                        help="The last date from which you'd like to process transactions. Defaults to the end of last month.")

    args = parser.parse_args()

    # If config file is provided, load it
    config_values = {}
    if args.config:
        config_values = load_config(args.config)

    # Convert args to dict, excluding None values
    cmd_args = {k: v for k, v in vars(args).items() if v is not None and k != 'config'}

    # Merge config and command line args (config line takes precedence)
    final_args = {**cmd_args, **config_values}

    # Validate required arguments
    required_args = ['api_key', 'email', 'password']
    missing_args = [arg for arg in required_args if arg not in final_args]
    if missing_args:
        parser.error(f"Missing required arguments: {', '.join(missing_args)}")

    return final_args


# Usage

async def main():
    args = parse_args()
    category_ids = args.get('category_ids')
    api_key = args['api_key']
    csv_name = './Your Orders/Retail.OrderHistory.1/Retail.OrderHistory.1.csv'
    digital_items_csv_name = './Your Orders/Digital-Ordering.1/Digital Items.csv'
    digital_transaction_csv_name = './Your Orders/Digital-Ordering.1/Digital Orders Monetary.csv'
    email = args['email']
    password = args['password']
    sleep_seconds = float(args.get('sleep_seconds', 1.0))
    start_date = args['start_date']
    end_date = args['end_date']

    print(f"Monarch Category IDs: {category_ids}")
    print(f"Anthropic API Key: {api_key}")
    print(f"CSV Name: {csv_name}")
    print(f"Digital CSV Name: {digital_items_csv_name}")
    print(f"Digital Transaction CSV Name: {digital_transaction_csv_name}")
    print(f"Monarch Email: {email}")
    print(f"Monarch Password: {password}")
    print(f"Sleep seconds: {sleep_seconds}")
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")

    mm = MonarchMoney()
    client = AsyncAnthropic(api_key=api_key)

    await mm.login(email, password)
    await match_and_update_transactions(mm, client, csv_name, digital_items_csv_name, digital_transaction_csv_name,
                                        category_ids, sleep_seconds,
                                        start_date, end_date)


if __name__ == '__main__':
    print(f"Script version: {__version__}")

    asyncio.run(main())

    # todo differentiate between positive and negative transactions, to allow for returns categorization.
