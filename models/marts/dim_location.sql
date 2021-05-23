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

,   selected3 as (
        select
            countryregioncode
            , name	as country
            , modifieddate	

        from {{ref('stg_countryregion')}}            
    )

,   final1 as (
        select
            selected1.addressid
            , selected1.stateprovinceid
            , selected2.territoryid
            , selected1.city	
            , selected1.addressline1
            , selected1.addressline2	
            , selected1.postalcode	
            , selected1.spatiallocation
            , selected2.stateprovince
            , selected3.country
            , selected2.stateprovincecode
            , selected2.countryregioncode
            , selected2.isonlystateprovinceflag
            , selected1.rowguid
            , selected1.modifieddate           		
            from selected1
            left join selected2 on selected1.stateprovinceid = selected2.stateprovinceid
            left join selected3 on selected3.countryregioncode = selected2.countryregioncode 
)

,   final2 as (
        select
        row_number () over (order by addressid) as location_sk
        , *
    from final1
    )

select *
from final2