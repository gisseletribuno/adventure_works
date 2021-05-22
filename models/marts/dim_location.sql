with
    selected1 as (
        select
            addressid	
            , stateprovinceid	
            , city	
            , addressline1
            , addressline2	
            , postalcode	
            , spatiallocation
            , rowguid
            , modifieddate		

        from {{ref('stg_address')}}
    )
,   transformed1 as (
        select
        row_number () over (order by addressid) as address_sk
        , *
    from selected1
)

,   selected2 as (
        select
            stateprovinceid
            , territoryid
            , name as stateprovince
            , stateprovincecode
            , countryregioncode
            , isonlystateprovinceflag
            , rowguid

        from {{ref('stg_stateprovince')}}            
    )
,   transformed2 as (
        select
        row_number () over (order by territoryid) as location_sk
        , *
    from selected2
)

,   selected3 as (
        select
            countryregioncode
            , name	as country
            , modifieddate	

        from {{ref('stg_countryregion')}}            
    )
,   transformed3 as (
        select
        row_number () over (order by countryregioncode) as countryregioncode_sk
        , *
    from selected3
)

,   final as (
        select
            transformed1.stateprovinceid
            , transformed1.addressid
            , transformed2.location_sk
            , transformed1.city	
            , transformed1.addressline1
            , transformed1.addressline2	
            , transformed1.postalcode	
            , transformed1.spatiallocation
            , transformed2.stateprovince
            , transformed3.country
            , transformed2.stateprovincecode
            , transformed2.countryregioncode
            , transformed2.isonlystateprovinceflag
            , transformed1.rowguid
            , transformed1.modifieddate           		
            from transformed1
            left join transformed2 on transformed1.stateprovinceid = transformed2.stateprovinceid
            left join transformed3 on transformed3.countryregioncode = transformed2.countryregioncode 
)

select *
from final