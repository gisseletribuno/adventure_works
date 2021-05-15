with
    stg_customer as (
        select
            /* Primary key */
            customerid

            /* Foreign key */
            , personid
            , territoryid
            , storeid
            , rowguid
           
            , modifieddate	

            /* Stich columns */
            , _sdc_table_version
            , _sdc_received_at
            , _sdc_sequence
            , _sdc_batched_at	
            , _sdc_extracted_at as last_etl_run


        from {{source('adventure_works', 'customer')}}
    )

select *
from stg_customer