with
    selected as (
        select
            productid
            , productmodelid
            , productsubcategoryid 
            , name
            , sellenddate
            , safetystocklevel
            , finishedgoodsflag
            , class
            , makeflag
            , productnumber
            , reorderpoint
            , modifieddate
            , rowguid
            , weightunitmeasurecode
            , standardcost
            , style
            , sizeunitmeasurecode
            , listprice
            , daystomanufacture
            , productline
            , size
            , color
            , sellstartdate
            , weight	

        from {{ref('stg_product')}}
    )
,   transformed as (
        select
        row_number () over (order by productid) as product_sk
        , *
    from selected
    )

select *
from transformed