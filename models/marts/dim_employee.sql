with
    selected1 as (
        select
            businessentityid	
            , territoryid
            , salesquota
            , saleslastyear
            , salesytd
            , commissionpct
            , bonus
            , modifieddate		
            , rowguid			

        from {{ref('stg_salesperson')}}
    )
,   transformed1 as (
        select
        row_number () over (order by businessentityid) as employee_sk
        , *
    from selected1
)

,   selected2 as (
        select
            businessentityid
            , nationalidnumber
            , loginid	
            , jobtitle
            , birthdate	        
            , gender	
            , maritalstatus
            , hiredate	
            , salariedflag	
            , sickleavehours	
            , organizationnode		
            , vacationhours
            , currentflag
            , modifieddate	
            , rowguid	      

        from {{ref('stg_employee')}}            
    )
,   transformed2 as (
        select
        row_number () over (order by businessentityid) as employee_sk
        , *
    from selected2
)

,   selected3 as (
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
,   transformed3 as (
        select
        row_number () over (order by businessentityid) as person_sk
        , *
    from selected3
)

,   final as (
        select
            transformed1.businessentityid
            , transformed2.nationalidnumber
            , transformed2.loginid	
            , transformed2.jobtitle
            , transformed2.birthdate	        
            , transformed2.gender	
            , transformed2.maritalstatus
            , transformed2.hiredate	
            , transformed3.title
            , transformed3.lastname
            , transformed3.firstname
            , transformed3.middlename
            , transformed3.emailpromotion
            , transformed3.persontype
            , transformed3.namestyle
            , transformed3.suffix
            , transformed2.salariedflag	
            , transformed2.sickleavehours	
            , transformed2.organizationnode		
            , transformed2.vacationhours
            , transformed2.currentflag
            , transformed2.modifieddate	
            , transformed2.rowguid	      
            , transformed1.territoryid
            , transformed1.salesquota
            , transformed1.saleslastyear
            , transformed1.salesytd
            , transformed1.commissionpct
            , transformed1.bonus
            from transformed1
            left join transformed2 on transformed1.businessentityid = transformed2.businessentityid	
            left join transformed3 on transformed2.businessentityid = transformed3.businessentityid	        
)

select *
from final