DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.transaction;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.transaction
(
    operation_id        varchar(100),
    account_number_from double precision,
    account_number_to   double precision,
    currency_code       bigint,
    country             varchar(100),
    status              varchar(100),
    transaction_type    varchar(100),
    transaction_dt      datetime,
    trigger_dttm        datetime,
    update_dttm         datetime

)
    ORDER BY operation_id
    SEGMENTED BY hash(operation_id) ALL NODES;

