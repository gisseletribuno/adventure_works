with
    stg_creditcard as (
        select
            /* Primary key */
            creditcardid

            , cardtype
            , cardnumber
            , expmonth
            , expyear	
            , modifieddate	
            
            /* Stich columns */
            , _sdc_table_version	
            , _sdc_received_at	
            , _sdc_sequence
            , _sdc_batched_at	
            , _sdc_extracted_at	as last_etl_run


        from {{source('adventure_works', 'creditcard')}}
    )

select *
from stg_creditcard