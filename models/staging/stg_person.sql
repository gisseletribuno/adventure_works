with
    stg_person as (
        select
            /* Primary key */
            rowguid

            /* Foreign key */
            , businessentityid

            , title
            , lastname
            , firstname
            , middlename
            , emailpromotion
            , persontype
            , namestyle
            , suffix
            , modifieddate

            /* Stich columns */
            , _sdc_table_version
            , _sdc_received_at
            , _sdc_sequence
            , _sdc_batched_at
            , _sdc_extracted_at as last_etl_run


        from {{source('adventure_works', 'person')}}
    )

select *
from stg_person