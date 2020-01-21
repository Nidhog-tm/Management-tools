#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import boto3
from datetime import datetime, timedelta, date
from urllib.parse import parse_qs


def lambda_handler(event, context):
    client = boto3.client('ce', region_name='us-east-1')
    token = os.environ['SLACK_TOKEN']
    query = parse_qs(event.get('body') or '')
    if query.get('token', [''])[0] != token:
        # 予期しない呼び出し。400 Bad Requestを返す
        return {'statusCode': 400}
    slackbot_name = 'slackbot'
    if query.get('user_name', [slackbot_name])[0] == slackbot_name:
        # Botによる書き込み。無限ループを避けるために、何も書き込まない
        return {'statusCode': 200}
    # textの内容をそのまま書き込む
    # return {
    # 'statusCode': 200,
    # 'body': json.dumps({
    #     'text': query.get('text', [''])[0]
    # }) }

    # 合計とサービス毎の請求額を取得する
    total_billing = get_total_billing(client)
    service_billings = get_service_billings(client)
    
    return {
        'statusCode': 200,
        'body': json.dumps(get_message(total_billing, service_billings))}


# 合計請求額取得を取得
def get_total_billing(client) -> dict:
    (start_date, end_date) = get_total_cost_date_range()
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=[
            'AmortizedCost'
        ]
    )
    
    return {
        'start': response['ResultsByTime'][0]['TimePeriod']['Start'],
        'end': response['ResultsByTime'][0]['TimePeriod']['End'],
        'billing': response['ResultsByTime'][0]['Total']['AmortizedCost']['Amount'],
    }


# 各サービスの詳細請求金額を取得
def get_service_billings(client) -> list:
    (start_date, end_date) = get_total_cost_date_range()

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ce.html#CostExplorer.Client.get_cost_and_usage
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=[
            'AmortizedCost'
        ],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
        ]
    )

    billings = []

    for item in response['ResultsByTime'][0]['Groups']:
        billings.append({
            'service_name': item['Keys'][0],
            'billing': item['Metrics']['AmortizedCost']['Amount']
        })
    return billings


# 請求金額取得対象期間を取得
def get_total_cost_date_range() -> (str, str):
    start_date = get_begin_of_month()
    end_date = get_today()

    # get_cost_and_usage()のstartとendに同じ日付は指定不可のため、
    # 「今日が1日」なら、「先月1日から今月1日（今日）」までの範囲にする
    if start_date == end_date:
        end_of_month = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=-1)
        begin_of_month = end_of_month.replace(day=1)
        return begin_of_month.date().isoformat(), end_date
    return start_date, end_date


def get_message(total_billing: dict, service_billings: list) -> dict:
    start = datetime.strptime(total_billing['start'], '%Y-%m-%d').strftime('%m/%d')
 
    # Endの日付は結果に含まないため、表示上は前日にしておく
    end_today = datetime.strptime(total_billing['end'], '%Y-%m-%d')
    end_yesterday = (end_today - timedelta(days=1)).strftime('%m/%d')
 
    total = round(float(total_billing['billing']), 2)
 
    title = f'{start}～{end_yesterday}の請求額は、{total:.2f} USDです。'
 
    details = []
    for item in service_billings:
        service_name = item['service_name']
        billing = round(float(item['billing']), 2)
 
        if billing == 0.0:
            # 請求無し（0.0 USD）の場合は、内訳を表示しない
            continue
        details.append(f'　・{service_name}: {billing:.2f} USD')
    
    payload = {
        'attachments': [
            {
                'color': '#36a64f',
                'pretext': title,
                'text': '\n'.join(details)
            }
        ]
    }
    return payload


# 実行月の1日を取得
def get_begin_of_month() -> str:
    return date.today().replace(day=1).isoformat()


# 実行日を取得
def get_today() -> str:
    return date.today().isoformat()

