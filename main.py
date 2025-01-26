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
                       row['TransactionAmount']
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


def parse_data(csv_file, digital_order_csv, is_digital_order_data_set=False):
    # todo type hint this
    orderIdIndex = 0 if is_digital_order_data_set else 1
    subTotalIndex = 4 if is_digital_order_data_set else 9
    descriptionIndex = 1 if is_digital_order_data_set else 23
    deliveryDateIndex = 2 if is_digital_order_data_set else 18
    orderDateIndex = 3 if is_digital_order_data_set else 2

    def str_to_float(string, is_digital_order):
        if not is_digital_order:
            # Remove commas from the string
            string = string.replace(',', '')

        # Convert the modified string to a float
        return round(float(string), 2)

    orders = defaultdict(dict)

    pprint(is_digital_order_data_set);
    with open(csv_file, 'r') as file:
        if not is_digital_order_data_set:
            data = csv.reader(file)
            next(data)  # Skip the header row
        else:
            data = process_csv_files(csv_file, digital_order_csv)
        for row in data:
            print(f'Raw row: {row}')
            if not is_digital_order_data_set and row[16] == 'Cancelled':
                continue
            order_id = f'{row[orderIdIndex]}'
            item_subtotal = str_to_float(row[subTotalIndex], is_digital_order_data_set)
            if order_id in orders:
                orders[order_id]['total_cost'] += item_subtotal
                orders[order_id]['description'] += ' ' + row[descriptionIndex]
            else:
                orders[order_id] = {'total_cost': item_subtotal, 'description': row[descriptionIndex],
                                    'delivery_date': row[deliveryDateIndex], 'order_date': row[orderDateIndex]}
        return orders


async def match_and_update_transactions(mm: MonarchMoney, anthropic_client: any, csv_file: str,
                                        digital_transact_csv: str,
                                        category_ids: List[str], sleep_seconds: float, is_digital_order: bool) -> None:
    unmatched_rows: List[Tuple[str, str, float]] = []
    mm_categories = await mm.get_transaction_categories()
    cat_names, cat_map = process_categories(mm_categories)
    pprint("category list")
    pprint(cat_map)

    orders = parse_data(csv_file, digital_transact_csv, is_digital_order)
    for order_id, items in orders.items():

        pprint(f'orderID: {order_id}, {items}')
        if items['delivery_date'] == 'Not Available':
            continue
        try:
            transaction_date_start = (datetime.strptime(items['order_date'], '%Y-%m-%dT%H:%M:%SZ') - timedelta(
                days=1)).strftime('%Y-%m-%d')

        except ValueError as e:
            pprint(e)
            transaction_date_start = (datetime.strptime(items['order_date'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(
                days=1)).strftime('%Y-%m-%d')

        try:
            transaction_date_end = (
                    datetime.strptime(items['delivery_date'], '%Y-%m-%dT%H:%M:%SZ') + timedelta(days=4)).strftime(
                '%Y-%m-%d')
        except ValueError as e:
            pprint(e)
            try:
                transaction_date_end = (
                        datetime.strptime(items['delivery_date'], '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(
                    days=4)).strftime('%Y-%m-%d')
            except ValueError as e:
                pprint(e)
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

            pprint(f'Checked for transactions from {transaction_date_start} to {transaction_date_end}')
            pprint(f'transactions found for comparison: {len(transactions)}')
            for transaction in transactions:
                rounded_transaction = round(abs(transaction['amount']), 2)
                print(f'transaction amount unrounded: ${transaction['amount']}, rounded: ${rounded_transaction}, '
                      f'total_cost: ${total_cost}')
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
            print(f"Order Date: {row[0]}, Delivery Date: {row[1]}, Description: {row[2]}, Total Cost: ${row[3]}")


def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    # Convert config section to dict, stripping quotes if present
    return {k: v.strip("'\"") for k, v in config['DEFAULT'].items()}


def parse_args():
    parser = argparse.ArgumentParser(description='Process command-line arguments.')
    parser.add_argument('--config', help='Config file path for arguments')

    parser.add_argument('--category_ids', nargs='+', required=False,
                        help='Category IDs to filter (space-separated)')
    parser.add_argument('--api_key', required=False,  # Changed to False since it might come from config
                        help='Anthropic API key')
    parser.add_argument('--csv_name', required=False,
                        help='Name of the Amazon Order History CSV file or Digital Items.csv')
    parser.add_argument('--digital_transaction_csv_name', required=False,
                        help='The Digital Orders.csv. Contains the true transaction costs of digital orders.')
    parser.add_argument('--email', required=False,
                        help='Monarch email')
    parser.add_argument('--password', required=False,
                        help='Monarch password')
    parser.add_argument('--sleep_seconds', default=1.0,
                        help='Sleep seconds to mitigate rate limits by Anthropic')
    parser.add_argument('--is_digital_order',
                        help='Set to True if processing a digital order')

    args = parser.parse_args()

    # If config file is provided, load it
    config_values = {}
    if args.config:
        config_values = load_config(args.config)

    # Convert args to dict, excluding None values
    cmd_args = {k: v for k, v in vars(args).items() if v is not None and k != 'config'}

    # Merge config and command line args (command line takes precedence)
    final_args = {**config_values, **cmd_args}

    # Validate required arguments
    required_args = ['api_key', 'csv_name', 'email', 'password']
    missing_args = [arg for arg in required_args if arg not in final_args]
    if missing_args:
        parser.error(f"Missing required arguments: {', '.join(missing_args)}")

    return final_args


# Usage

async def main():
    args = parse_args()
    category_ids = args.get('category_ids')
    api_key = args['api_key']
    csv_name = args['csv_name']
    digital_transaction_csv_name = args.get('digital_transaction_csv_name')
    email = args['email']
    password = args['password']
    sleep_seconds = float(args.get('sleep_seconds', 1.0))
    is_digital_order = args['is_digital_order'] == 'True'

    print(f"Monarch Category IDs: {category_ids}")
    print(f"Anthropic API Key: {api_key}")
    print(f"CSV Name: {csv_name}")
    print(f"Digital Transaction CSV Name: {digital_transaction_csv_name}")
    print(f"Monarch Email: {email}")
    print(f"Monarch Password: {password}")
    print(f"Sleep seconds: {sleep_seconds}")
    print(f"is digital order file?: {is_digital_order}")

    mm = MonarchMoney()
    client = AsyncAnthropic(api_key=api_key)

    await mm.login(email, password)
    await match_and_update_transactions(mm, client, csv_name, digital_transaction_csv_name, category_ids, sleep_seconds,
                                        is_digital_order)


if __name__ == '__main__':
    asyncio.run(main())

    # todo differentiate between positive and negative transactions, to allow for returns categorization.
