with
    stg_salesperson as (
        select
            /* Primary key */
            businessentityid
            	
            /* Foreign key */	
            , territoryid
            
            , salesquota
            , saleslastyear
            , salesytd
            , commissionpct
            , bonus
            , modifieddate		
            , rowguid		
            	
            /* Stich columns */	
           ,  _sdc_table_version
           ,  _sdc_received_at	
           ,  _sdc_sequence	
           ,  _sdc_batched_at
           ,  _sdc_extracted_at as last_etl_run

        from {{source('adventure_works', 'salesperson')}}
    )

select *
from stg_salesperson