create table allpago_raw as
select unique_id,
allpago.email as email,
case when ra.recurring = 0 then 'New' when channel_name != 'Allpago_PSP_BR_Boleto' and ra.recurring > 0 then 'Recurring' else null end as Recurring,
ra.recurring as recurring_number,
ip,
given_name,
country,
country_code,
state,
city,
region_code,
channel_name,
reason_code_meaning,
return_code_meaning,
status_code_meaning,
concat(concat(concat(concat(substring(allpago.transaction_Date,7,4),'-'),substring(allpago.transaction_Date,4,2)),'-'),substring(allpago.transaction_Date,0,3))::date as transaction_date,
credit,
debit,
allpago.currency, allpago.payment_method, payment_type, fees, taxes, netpayout, bulk_id, reference_id, status_code, reason_code, return_code, transaction_id, account_last_four_digits, brand, account_year, account_month, bin_number, issuing_bank, card_type, card_level, boleto_due_date, num_installments
from allpago_raw2 allpago
left join
receipts_aggregated ra on (ra.order_id = regexp_substr(allpago.transaction_id, '^\\d*') or ra.order_id = allpago.transaction_id) and date(ra.received) = concat(concat(concat(concat(substring(allpago.transaction_Date,7,4),'-'),substring(allpago.transaction_Date,4,2)),'-'),substring(allpago.transaction_Date,0,3))::date;
