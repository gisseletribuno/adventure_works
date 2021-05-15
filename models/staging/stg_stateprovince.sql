with
    stg_stateprovince as (
        select
            /* Primary key */
            stateprovinceid
            
            /* Foreign key */
            , rowguid
            , territoryid
            
            , name
            , stateprovincecode
            , countryregioncode
            , isonlystateprovinceflag
            , modifieddate
            
            /* Stich columns */
            , _sdc_table_version	
            , _sdc_received_at	
            , _sdc_sequence	
            , _sdc_batched_at	
            , _sdc_extracted_at as last_etl_run
            
 
        from {{source('adventure_works', 'stateprovince')}}
    )

select *
from stg_stateprovince