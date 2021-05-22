with
    stg_personcreditcard as (
        select
            /* Primary key */
            businessentityid	
            /* Foreign key */	
            , creditcardid	

            , modifieddate

            /* Stich columns */
            , _sdc_table_version	
            , _sdc_received_at	
            , _sdc_sequence
            , _sdc_batched_at	
            , _sdc_extracted_at	as last_etl_run


        from {{source('adventure_works', 'personcreditcard')}}
    )

select *
from stg_personcreditcard