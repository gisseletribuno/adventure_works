with
    stg_salesorderheaderreason as (
        select
            /* Primary key */
            salesreasonid	
            
            /* Foreign key */
            , salesorderid	
            
            , modifieddate
        

            /* Stich columns */
            , _sdc_received_at	
            , _sdc_sequence
            , _sdc_batched_at	
            , _sdc_table_version
            , _sdc_extracted_at as last_etl_run
            

        from {{source('adventure_works', 'salesorderheadersalesreason')}}
    )

select *
from stg_salesorderheaderreason