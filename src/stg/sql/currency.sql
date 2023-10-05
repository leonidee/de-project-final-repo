DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.currency;
CREATE TEMPORARY TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.currency
(
    object_id          varchar(100),
    date_update        datetime,
    currency_code      int,
    currency_code_with int,
    currency_with_div  int,
    trigger_dttm       datetime,
    update_dttm        datetime
)
    ORDER BY object_id
    SEGMENTED BY hash(object_id) ALL NODES;
