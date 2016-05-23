create table adyen_raw(
merchant2 varchar(25),
psp_ref bigint,
merchant_ref varchar(100),
payment_method varchar(250),
creation_timestamp timestamp,
timezone varchar(5),
currency varchar(15),
amount float,
risk_score int,
shopper_interaction varchar(100),
shopper_pan int,
ip varchar(50),
country varchar(50),
issuer_name varchar(250),
issuer_id varchar(250),
issuer_city varchar(50),
issuer_country varchar(40),
acquirer_response varchar(500),
shopper_email varchar(250),
shopper_reference varchar(250),
cvc2_response varchar(20),
AVS_response int,
billing_street varchar(50),
acquirer_reference varchar(100),
payment_card varchar(50),
raw_acquirer_response varchar(250)
);
