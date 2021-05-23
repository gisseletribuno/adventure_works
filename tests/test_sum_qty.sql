with    
    validation as (
        select sum(orderqty) as sum_val
        from {{ref ('facts_salesorderdetails')}}
        where duedate between '2014-06-01' and '2014-06-30'
    )

select * from validation where sum_val != 3889