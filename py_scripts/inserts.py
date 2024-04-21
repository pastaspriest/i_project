#!/usr//bin/python3

import pandas as pd

def transactions_insert(file, cursor):
	# Очищаем стейджинг
	cursor.execute( "delete from deaian.krph_stg_transactions;" )
	# Захват транзакций
	columns = ["trans_id", "trans_date", "amt", "card_num", "oper_type", "oper_result", "terminal"]
	df = pd.read_csv( "" + file, sep=";",header=None, names=columns, skiprows=1 )
	cursor.executemany( '''INSERT INTO deaian.krph_stg_transactions(
								trans_id,
								trans_date,
								amt,
								card_num,
								oper_type,
								oper_result,
								terminal)
			       			VALUES( %s, to_date(%s, 'yyyy-mm-dd'), cast(replace (%s, ',', '.') as decimal(18,2)), %s, %s, %s, %s )''', df.values.tolist() )

	# Вставляем транзакций в DWH. ДЕЛАЕМ ТОЛЬКО ВСТАВКУ!
	cursor.execute( '''
					insert into deaian.krph_dwh_fact_transactions ( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal )
					select
						stg.trans_id, 
						stg.trans_date, 
						stg.card_num, 
						stg.oper_type, 
						stg.amt,
						stg.oper_result,
						stg.terminal
					from deaian.krph_stg_transactions stg
					left join deaian.krph_dwh_fact_transactions dwh
					on stg.trans_id = dwh.trans_id
					where dwh.trans_id is null;
					''')

def blacklist_insert(file, cursor):
	# Очищаем стейджинг
	cursor.execute( "delete from deaian.krph_stg_passport_blacklist;" )
	# Захват ЧС паспортов
	df = pd.read_excel( 'data/' + file, sheet_name='blacklist', header=0, index_col=None )

	cursor.executemany( '''INSERT INTO deaian.krph_stg_passport_blacklist(
								entry_dt,
								passport_num)
			       			VALUES( to_date( cast(%s as varchar(10)), 'yyyy-mm-dd'), %s)''', df.values.tolist() )

	# Вставляем passport_blacklist в DWH. ДЕЛАЕМ ТОЛЬКО ВСТАВКУ!
	cursor.execute( '''
					insert into deaian.krph_dwh_fact_passport_blacklist ( passport_num, entry_dt )
					select
						stg.passport_num, 
						stg.entry_dt
					from deaian.krph_stg_passport_blacklist stg
					left join deaian.krph_dwh_fact_passport_blacklist dwh
					on stg.passport_num = dwh.passport_num
					where dwh.passport_num is null;
					''' )

def terminals_insert(file, cursor):
	# Очищаем стейджинг
	cursor.execute( "delete from deaian.krph_stg_terminals;" )
	cursor.execute( "delete from deaian.krph_stg_terminals_del;" )
	# Захват терминалов
	df = pd.read_excel( '/data/terminals_01032021.xlsx', sheet_name='terminals', header=0, index_col=None )

	cursor.executemany( '''INSERT INTO deaian.krph_stg_terminals(
								terminal_id,
								terminal_type,
								terminal_city,
								terminal_address,
								create_dt,
								update_dt)
			       			VALUES( %s, %s, %s, %s, now(), null)''', df.values.tolist() )

	cursor.execute( '''
							insert into deaian.krph_stg_terminals_del ( terminal_id )
							select terminal_id from deaian.krph_dwh_dim_terminals;
		''' )
	# Вставляем terminals в DWH.
	cursor.execute( '''
					insert into deaian.krph_dwh_dim_terminals ( terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt )
					select
						stg.terminal_id, 
						stg.terminal_type,
						stg.terminal_city,
						stg.terminal_address,
						stg.create_dt,
						stg.update_dt
					from deaian.krph_stg_terminals stg
					left join deaian.krph_dwh_dim_terminals dwh
					on stg.terminal_id = dwh.terminal_id
					where dwh.terminal_id is null;
					''' )
	# Обновление terminals
	cursor.execute( '''
					update deaian.krph_dwh_dim_terminals
					set 
						terminal_type = tmp.terminal_type,
						terminal_city = tmp.terminal_city,
						terminal_address = tmp.terminal_address,
						update_dt = now()
					from (
						select
							stg.terminal_id,
							stg.terminal_type,
							stg.terminal_city,
							stg.terminal_address
						from deaian.krph_stg_terminals stg
						inner join deaian.krph_dwh_dim_terminals dwh
						on stg.terminal_id = dwh.terminal_id
						where 
						stg.terminal_type <> dwh.terminal_type or (stg.terminal_type is null and dwh.terminal_type is not null) or (stg.terminal_type is not null and dwh.terminal_type is null)
						or stg.terminal_city <> dwh.terminal_city or (stg.terminal_city is null and dwh.terminal_city is not null) or (stg.terminal_city is not null and dwh.terminal_city is null)
						or stg.terminal_address <> dwh.terminal_address or (stg.terminal_address is null and dwh.terminal_address is not null) or (stg.terminal_address is not null and dwh.terminal_address is null)
					) tmp
					where krph_dwh_dim_terminals.terminal_id = tmp.terminal_id;
					''' )
	# Удаление terminals
	cursor.execute( '''
					delete from deaian.krph_dwh_dim_terminals
					where terminal_id in (
						select 
							stgd.terminal_id
						from deaian.krph_stg_terminals_del stgd
						left join deaian.krph_stg_terminals stg
						on stg.terminal_id = stgd.terminal_id
						where stg.terminal_id is null
					);
					''' )



def clients_insert(cursor, bank_cursor):
	# Очищаем стейджинг
	cursor.execute( "delete from deaian.krph_stg_clients;" )
	cursor.execute( "delete from deaian.krph_stg_clients_del;" )
	# Захват клиентов из базы BANK
	bank_cursor.execute( '''SELECT
	                        	client_id,
	                        	last_name,
	                        	first_name,
	                        	patronymic,
	                        	date_of_birth,
	                        	passport_num,
	                        	passport_valid_to,
	                        	phone,
								create_dt,
								update_dt
	                     FROM bank.info.clients''' )
	records = bank_cursor.fetchall()

	names = [ x[0] for x in bank_cursor.description ]
	df = pd.DataFrame( records, columns = names )

	cursor.executemany( '''INSERT INTO deaian.krph_stg_clients( 
								client_id,
								last_name,
								first_name,
								patronymic,
								date_of_birth,
								passport_num,
								passport_valid_to,
								phone,
								create_dt,
								update_dt)
			       			VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )''', df.values.tolist() )

	cursor.execute( '''
							insert into deaian.krph_stg_clients_del ( client_id )
							select client_id from deaian.krph_dwh_dim_clients;
		''' )
	# Вставляем clients в DWH
	cursor.execute( '''
					insert into deaian.krph_dwh_dim_clients ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt )
					select
						stg.client_id, 
						stg.last_name, 
						stg.first_name, 
						stg.patronymic, 
						stg.date_of_birth,
						stg.passport_num,
						stg.passport_valid_to,
						stg.phone,
						stg.create_dt,
						stg.update_dt
					from deaian.krph_stg_clients stg
					left join deaian.krph_dwh_dim_clients dwh
					on stg.client_id = dwh.client_id
					where dwh.client_id is null;
					''' )
	# Обновление clients
	cursor.execute( '''
					update deaian.krph_dwh_dim_clients
					set 
						last_name = tmp.last_name,
						first_name = tmp.first_name,
						patronymic = tmp.patronymic,
						date_of_birth = tmp.date_of_birth,
						passport_num = tmp.passport_num,
						passport_valid_to = tmp.passport_valid_to,
						phone = tmp.phone,
						update_dt = now()
					from (
						select
							stg.client_id, 
							stg.last_name, 
							stg.first_name, 
							stg.patronymic, 
							stg.date_of_birth,
							stg.passport_num,
							stg.passport_valid_to,
							stg.phone
						from deaian.krph_stg_clients stg
						inner join deaian.krph_dwh_dim_clients dwh
						on stg.client_id = dwh.client_id
						where 
						stg.last_name <> dwh.last_name or (stg.last_name is null and dwh.last_name is not null) or (stg.last_name is not null and dwh.last_name is null)
						or stg.first_name <> dwh.first_name or (stg.first_name is null and dwh.first_name is not null) or (stg.first_name is not null and dwh.first_name is null)
						or stg.patronymic <> dwh.patronymic or (stg.patronymic is null and dwh.patronymic is not null) or (stg.patronymic is not null and dwh.patronymic is null)
						or stg.date_of_birth <> dwh.date_of_birth or (stg.date_of_birth is null and dwh.date_of_birth is not null) or (stg.date_of_birth is not null and dwh.date_of_birth is null)
						or stg.passport_num <> dwh.passport_num or (stg.passport_num is null and dwh.passport_num is not null) or (stg.passport_num is not null and dwh.passport_num is null)
						or stg.passport_valid_to <> dwh.passport_valid_to or (stg.passport_valid_to is null and dwh.passport_valid_to is not null) or (stg.passport_valid_to is not null and dwh.passport_valid_to is null)
						or stg.phone <> dwh.phone or (stg.phone is null and dwh.phone is not null) or (stg.phone is not null and dwh.phone is null)
					) tmp
					where deaian.krph_dwh_dim_clients.client_id = tmp.client_id;
					''' )
	# Удаление clients
	cursor.execute( '''
					delete from deaian.krph_dwh_dim_clients
					where client_id in (
						select 
							stgd.client_id
						from deaian.krph_stg_clients_del stgd
						left join deaian.krph_stg_clients stg
						on stg.client_id = stgd.client_id
						where stg.client_id is null
					);
					''' )

def accounts_insert(cursor, bank_cursor):
	# Очищаем стейджинг
	cursor.execute( "delete from deaian.krph_stg_accounts;" )
	cursor.execute( "delete from deaian.krph_stg_accounts_del;" )
	# Захват аккаунтов из базы BANK
	bank_cursor.execute( '''SELECT
	                            account,
								valid_to,
								client,
								create_dt,
								update_dt
	                     	FROM bank.info.accounts''' )
	records = bank_cursor.fetchall()

	names = [ x[0] for x in bank_cursor.description ]
	df = pd.DataFrame( records, columns = names )

	cursor.executemany( '''INSERT INTO deaian.krph_stg_accounts(
	                        	account_num,
								valid_to,
								client,
								create_dt,
								update_dt)
	                       VALUES( %s, %s, %s, %s, %s )''', df.values.tolist() )

	cursor.execute( '''
							insert into deaian.krph_stg_accounts_del ( account_num )
							select account_num from deaian.krph_dwh_dim_accounts;
		''' )
	# Вставляем Accounts в DWH
	cursor.execute( '''
					insert into deaian.krph_dwh_dim_accounts ( account_num, valid_to, client, create_dt, update_dt )
					select
						stg.account_num, 
						stg.valid_to, 
						stg.client, 
						stg.create_dt,
						stg.update_dt
					from deaian.krph_stg_accounts stg
					left join deaian.krph_dwh_dim_accounts dwh
					on stg.account_num = dwh.account_num
					where dwh.account_num is null;
					''' )
	# Обновление accounts
	cursor.execute( ''' 
					update deaian.krph_dwh_dim_accounts
					set 
						valid_to = tmp.valid_to,
						client = tmp.client,
						update_dt = now()
					from (
						select
							stg.account_num, 
							stg.valid_to, 
							stg.client
						from deaian.krph_stg_accounts stg
						inner join deaian.krph_dwh_dim_accounts dwh
						on stg.account_num = dwh.account_num
						where 
						stg.valid_to <> dwh.valid_to or (stg.valid_to is null and dwh.valid_to is not null) or (stg.valid_to is not null and dwh.valid_to is null)
						or stg.client <> dwh.client or (stg.client is null and dwh.client is not null) or (stg.client is not null and dwh.client is null)
					) tmp
					where deaian.krph_dwh_dim_accounts.account_num = tmp.account_num;
					''' )
	# Удаление accounts
	cursor.execute( '''
					delete from deaian.krph_dwh_dim_accounts
					where account_num in (
						select 
							stgd.account_num
						from deaian.krph_stg_accounts_del stgd
						left join deaian.krph_stg_accounts stg
						on stg.account_num = stgd.account_num
						where stg.account_num is null
					);
					''' )

def cards_insert(cursor, bank_cursor):
	# Очищаем стейджинг
	cursor.execute( "delete from deaian.krph_stg_cards;" )
	cursor.execute( "delete from deaian.krph_stg_cards_del;" )
	# Захват карт из базы BANK
	bank_cursor.execute( '''SELECT
	                            trim(card_num),
	                            account,
	                            create_dt,
	                            update_dt
	                     	FROM bank.info.cards''' )
	records = bank_cursor.fetchall()

	names = [ x[0] for x in bank_cursor.description ]
	df = pd.DataFrame( records, columns = names )

	cursor.executemany( '''INSERT INTO deaian.krph_stg_cards(
	                            card_num,
	                            account_num,
	                            create_dt,
	                            update_dt)
	                       VALUES( %s, %s, %s, %s )''', df.values.tolist() )

	cursor.execute( '''
							insert into deaian.krph_stg_cards_del ( card_num )
							select card_num from deaian.krph_dwh_dim_cards;
		''' )
	# Вставляем Cards в DWH
	cursor.execute( '''
					insert into deaian.krph_dwh_dim_cards ( card_num, account_num, create_dt, update_dt )
					select
						stg.card_num, 
						stg.account_num, 
						stg.create_dt,
						stg.update_dt
					from deaian.krph_stg_cards stg
					left join deaian.krph_dwh_dim_cards dwh
					on stg.card_num = dwh.card_num
					where dwh.card_num is null;
					''' )
	# Обновляем Cards в DWH
	cursor.execute( '''
				update deaian.krph_dwh_dim_cards
				set 
					account_num = tmp.account_num,
					update_dt = now()
				from (
					select
						stg.card_num,
						stg.account_num
					from deaian.krph_stg_cards stg
					inner join deaian.krph_dwh_dim_cards dwh
					on stg.card_num = dwh.card_num
					where 
					stg.account_num <> dwh.account_num or (stg.account_num is null and dwh.account_num is not null) or (stg.account_num is not null and dwh.account_num is null)
				) tmp
				where deaian.krph_dwh_dim_cards.card_num = tmp.card_num;
				''' )
	# Удаление cards
	cursor.execute( '''
					delete from deaian.krph_dwh_dim_cards
					where card_num in (
						select 
							stgd.card_num
						from deaian.krph_stg_cards_del stgd
						left join deaian.krph_stg_cards stg
						on stg.card_num = stgd.card_num
						where stg.card_num is null
					);
					''' )