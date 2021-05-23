with
    stg_businessentitycontact as (
        select
            /* Primary key */
            personid	
            , businessentityid
            , contacttypeid	

            , modifieddate	
            , rowguid	

            /* Stich columns */
            , _sdc_table_version
            , _sdc_received_at
            , _sdc_sequence
            , _sdc_batched_at
            , _sdc_extracted_at as last_etl_run


        from {{source('adventure_works', 'businessentitycontact')}}
    )

select *
from stg_businessentitycontact