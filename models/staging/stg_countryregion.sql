with
    stg_countryregion as (
        select
            /* Primary key */
            countryregioncode
            
            , name	
            , modifieddate	
            	
            /* Stich columns */
            , _sdc_received_at	
            , _sdc_sequence
            , _sdc_batched_at	
            , _sdc_table_version	
            , _sdc_extracted_at as last_etl_run

            
        from {{source('adventure_works', 'countryregion')}}
    )

select *
from stg_countryregion