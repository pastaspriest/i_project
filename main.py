#!/usr//bin/python3

import psycopg2
import pandas as pd
import os
from py_scripts.inserts import transactions_insert, blacklist_insert, terminals_insert, \
clients_insert, accounts_insert, cards_insert
from datetime import datetime


# Создание подключения к db BANK
bank_conn = psycopg2.connect(database = "bank",
                             host     = "",
                             user     = "",
                             password = "",
                             port     = "5432")

# Создание подключения к db EDU
conn = psycopg2.connect(database = "",
                        host     = "",
                        user     = "",
                        password = "",
                        port     = "5432")

# Отключение автокоммита
bank_conn.autocommit = False

# Создание курсора
bank_cursor = bank_conn.cursor()

# Отключение автокоммита
conn.autocommit = False

# Создание курсора
cursor = conn.cursor()

def insert_invalid_contract_operations(file):
	report_dt = file[-8:-4] + file[-10:-8] + file[-12:-10]
	cursor.execute( f'''
					insert into deaian.krph_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
					select 
						t.trans_date,
						cl.passport_num,
						cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic,
						cl.phone,
						'2',
						cast(\'{report_dt}\' as date)
					from deaian.krph_dwh_fact_transactions t
					inner join deaian.krph_dwh_dim_cards c
					on t.card_num  = c.card_num
					inner join deaian.krph_dwh_dim_accounts a
					on c.account_num = a.account_num 
					inner join deaian.krph_dwh_dim_clients cl
					on cl.client_id = a.client 
					where a.valid_to < t.trans_date 
					--and cast(\'{report_dt}\' as date) not in (select report_dt from deaian.krph_rep_fraud)
					;
	 				''' )

def insert_blocked_passport_operations(file):
	report_dt = file[-8:-4] + file[-10:-8] + file[-12:-10]
	cursor.execute( f'''
					insert into deaian.krph_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
					select 
						t.trans_date,
						cl.passport_num,
						cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic,
						cl.phone,
						'1',
						cast(\'{report_dt}\' as date)
					from deaian.krph_dwh_fact_transactions t
					inner join deaian.krph_dwh_dim_cards c
					on t.card_num  = c.card_num
					inner join deaian.krph_dwh_dim_accounts a
					on c.account_num = a.account_num 
					left join deaian.krph_dwh_dim_clients cl
					on cl.client_id = a.client and coalesce(cl.passport_valid_to, to_date('2900-01-01', 'yyyy-mm-dd')) < t.trans_date
					left join deaian.krph_dwh_fact_passport_blacklist p
					on cl.passport_num = p.passport_num and p.entry_dt < t.trans_date 
					where cl.client_id is not null or p.passport_num is not null
					--and cast(\'{report_dt}\' as date) not in (select report_dt from deaian.krph_rep_fraud);
	 				''' )

# Указываем путь к директории
directory = ""
# Получаем сортированный список файлов
files = sorted(os.listdir(directory))

files2 = [[],[],[]]

for f in files:
	if 'transactions' in f:
		files2[0].append(f)
	elif 'passport_blacklist' in f:
		files2[1].append(f)
	elif 'terminals' in f:
		files2[2].append(f)

if files2[0]:
	for f in range(len(files2)):
			transactions_insert(files2[0][f], cursor)
			blacklist_insert(files2[1][f], cursor)
			terminals_insert(files2[2][f], cursor)
	clients_insert(cursor, bank_cursor)
	accounts_insert(cursor, bank_cursor)
	cards_insert(cursor, bank_cursor)
	insert_blocked_passport_operations(files2[0][f])
	insert_invalid_contract_operations(files2[0][f])
else:
	print('Нет новых файлов', datetime.now())


conn.commit()

# Перемещаем файлы в архив
if files2[0]:
	for f in files:
		os.rename('' + f, '' + f + '.backup')

# Закрываем соединение BANK
bank_cursor.close()
bank_conn.close()

# Закрываем соединение EDU
cursor.close()
conn.close()
