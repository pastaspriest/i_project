create table deaian.krph_stg_transactions (
	trans_id varchar(11),
	trans_date date,
	amt decimal(18,2),
	card_num varchar(20), -- FK krph_stg_cards card_num
	oper_type varchar(8),
	oper_result varchar(7),
	terminal varchar(5) -- FK krph_stg_terminals terminal_id
);

create table deaian.krph_dwh_fact_transactions (
	trans_id varchar(11),
	trans_date date,
	card_num varchar(20), -- FK krph_dwh_dim_cards card_num
	oper_type varchar(8),
	amt decimal(18,2),
	oper_result varchar(7),
	terminal varchar(5) -- FK krph_dwh_dim_terminals terminal_id	
);

create table deaian.krph_stg_terminals (
	terminal_id varchar(5), -- PK krph_stg_transactions terminal
	terminal_type varchar(3),
	terminal_city varchar(40),
	terminal_address varchar(200),
	create_dt date,
	update_dt date
);

create table deaian.krph_stg_terminals_del (
	terminal_id varchar(5)
);

create table deaian.krph_dwh_dim_terminals (
	terminal_id varchar(5), -- PK krph_dwh_fact_transactions terminal
	terminal_type varchar(3),
	terminal_city varchar(40),
	terminal_address varchar(200),
	create_dt date,
	update_dt date
);

create table deaian.krph_stg_passport_blacklist (
	entry_dt date,
	passport_num varchar(11)
);

create table deaian.krph_dwh_fact_passport_blacklist (
	entry_dt date,
	passport_num varchar(11)
);

create table deaian.krph_stg_clients (
	client_id varchar(8), -- PK krph_stg_accounts client
	last_name varchar(50),
	first_name varchar(50),
	patronymic varchar(50),
	date_of_birth date,
	passport_num varchar(11),
	passport_valid_to date,
	phone varchar(16),
	create_dt date,
	update_dt date
);

create table deaian.krph_stg_clients_del (
	client_id varchar(8)
);

create table deaian.krph_dwh_dim_clients (
	client_id varchar(8), -- PK krph_dwh_dim_accounts client
	last_name varchar(50),
	first_name varchar(50),
	patronymic varchar(50),
	date_of_birth date,
	passport_num varchar(11),
	passport_valid_to date,
	phone varchar(16),
	create_dt date,
	update_dt date
);

create table deaian.krph_stg_accounts (
	account_num varchar(20), -- PK krph_stg_cards account_num
	valid_to date,
	client varchar(8), -- FK krph_stg_clients client_id
	create_dt date,
	update_dt date
);

create table deaian.krph_stg_accounts_del (
	account_num varchar(20)
);

create table deaian.krph_dwh_dim_accounts (
	account_num varchar(20), -- PK krph_dwh_dim_cards account_num
	valid_to date,
	client varchar(8), -- FK krph_dwh_dim_clients client_id
	create_dt date,
	update_dt date
);

create table deaian.krph_stg_cards (
	card_num varchar(20), -- PK krph_stg_transactions card_num
	account_num varchar(20), -- FK krph_stg_accounts account_num
	create_dt date,
	update_dt date
);

create table deaian.krph_stg_cards_del (
	card_num varchar(20)
);

create table deaian.krph_dwh_dimcards (
	card_num varchar(20), -- PK krph_dwh_fact_transactions card_num
	account_num varchar(20), -- FK krph_dwh_dim_accounts account_num
	create_dt date,
	update_dt date
);

create table deaian.krph_rep_fraud( 
	event_dt date, 
	passport varchar(11), 
	fio varchar(150), 
	phone varchar(16), 
	event_type char(1), 
	report_dt date
);