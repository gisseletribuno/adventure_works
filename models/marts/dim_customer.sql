with
    selected1 as (
        select
            customerid
            , personid
            , territoryid
            , storeid
            , rowguid
            , modifieddate	

        from {{ref('stg_customer')}}
    )
,   transformed1 as (
        select
        row_number () over (order by customerid) as customer_sk
        , *
    from selected1
)

,   selected2 as (
        select
            businessentityid
            , title
            , lastname
            , firstname
            , middlename
            , rowguid
            , emailpromotion
            , persontype
            , namestyle
            , suffix
            , modifieddate

        from {{ref('stg_person')}}            
    )
,   transformed2 as (
        select
        row_number () over (order by businessentityid) as person_sk
        , *
    from selected2
)

,   final as (
        select
            transformed1.personid
            , transformed1.customerid
            , transformed1.territoryid
            , transformed1.storeid
            , transformed2.businessentityid
            , transformed2.title
            , transformed2.lastname
            , transformed2.firstname
            , transformed2.middlename
            , transformed2.suffix
            , transformed2.emailpromotion
            , transformed2.persontype
            , transformed2.namestyle
            , transformed1.rowguid
            , transformed1.modifieddate
            from transformed1
            left join transformed2 on transformed1.personid = transformed2.businessentityid	        
)

select *
from final