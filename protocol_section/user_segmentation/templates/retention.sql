
-- with 
-- not breaking into multiples
with 
segmentation as (
    select 
        user,
        segment
    from query_{{segment_query}}
),
cohort_sizes as (
    select 
        protocol,
        cohort_summary.cohort_month,
        coalesce(u.segment, {{fallback_value}}) as segment,
        count(distinct cohort_summary.user) as cohort_size
    from dune.pyor_xyz.result_arb_metrics_actions_retention_query_final_test cohort_summary
        left join segmentation u 
            on cohort_summary.user = u.user
    where cohort_summary.cohort_month = cohort_summary.activity_month
    -- and protocol = 'Camelot'
    group by 1,2,3
),

returning_users as (
    select
        protocol,
        cohort_summary.cohort_month,
        coalesce(u.segment, {{fallback_value}}) as segment,
        date_diff('month', cohort_summary.cohort_month,cohort_summary.activity_month) as retention_month,
        count(case when cohort_summary.transaction_count > 0 then 1 end) as count_of_returning_users
    from dune.pyor_xyz.result_arb_metrics_actions_retention_query_final_test cohort_summary
        left join segmentation u 
            on cohort_summary.user = u.user
    -- where protocol = 'Camelot'
    group by 1, 2, 3, 4
),

returning_cohorts as (
    select
        returning_users.protocol,
        returning_users.segment,
        returning_users.cohort_month,
        returning_users.retention_month,
        returning_users.count_of_returning_users,
        (cast(100 as double) * cast(returning_users.count_of_returning_users as double) / cast(cohort_sizes.cohort_size as double)) as retention_percentage
    from returning_users
    left join cohort_sizes
        on returning_users.cohort_month = cohort_sizes.cohort_month
        and returning_users.protocol = cohort_sizes.protocol
        and returning_users.segment = cohort_sizes.segment
),

retention_percentage as (

    select 
    date(cohort_month) as cohort,
    protocol,
    segment,
    sum(case when retention_month = 0 then count_of_returning_users end) as absolute_count,
    
        sum(case when retention_month = 0 then cast(retention_percentage as double) end) as M0
        ,
    
        sum(case when retention_month = 1 then cast(retention_percentage as double) end) as M1
        ,
    
        sum(case when retention_month = 2 then cast(retention_percentage as double) end) as M2
        ,
    
        sum(case when retention_month = 3 then cast(retention_percentage as double) end) as M3
        ,
    
        sum(case when retention_month = 4 then cast(retention_percentage as double) end) as M4
        ,
    
        sum(case when retention_month = 5 then cast(retention_percentage as double) end) as M5
        ,
    
        sum(case when retention_month = 6 then cast(retention_percentage as double) end) as M6
        ,
    
        sum(case when retention_month = 7 then cast(retention_percentage as double) end) as M7
        ,
    
        sum(case when retention_month = 8 then cast(retention_percentage as double) end) as M8
        ,
    
        sum(case when retention_month = 9 then cast(retention_percentage as double) end) as M9
        ,
    
        sum(case when retention_month = 10 then cast(retention_percentage as double) end) as M10
        ,
    
        sum(case when retention_month = 11 then cast(retention_percentage as double) end) as M11
        ,
    
        sum(case when retention_month = 12 then cast(retention_percentage as double) end) as M12
        ,
    
        sum(case when retention_month = 13 then cast(retention_percentage as double) end) as M13
        ,
    
        sum(case when retention_month = 14 then cast(retention_percentage as double) end) as M14
        ,
    
        sum(case when retention_month = 15 then cast(retention_percentage as double) end) as M15
        ,
    
        sum(case when retention_month = 16 then cast(retention_percentage as double) end) as M16
        ,
    
        sum(case when retention_month = 17 then cast(retention_percentage as double) end) as M17
        ,
    
        sum(case when retention_month = 18 then cast(retention_percentage as double) end) as M18
        ,
    
        sum(case when retention_month = 19 then cast(retention_percentage as double) end) as M19
        -- ,
    
        -- sum(case when retention_month = 20 then cast(retention_percentage as double) end) as M20
        -- ,
    
        -- sum(case when retention_month = 21 then cast(retention_percentage as double) end) as M21
        -- ,
    
        -- sum(case when retention_month = 22 then cast(retention_percentage as double) end) as M22
        -- ,
    
        -- sum(case when retention_month = 23 then cast(retention_percentage as double) end) as M23
        -- ,
    
        -- sum(case when retention_month = 24 then cast(retention_percentage as double) end) as M24
        -- ,
    
        -- sum(case when retention_month = 25 then cast(retention_percentage as double) end) as M25
        -- ,
    
        -- sum(case when retention_month = 26 then cast(retention_percentage as double) end) as M26
        -- ,
    
        -- sum(case when retention_month = 27 then cast(retention_percentage as double) end) as M27
        -- ,
    
        -- sum(case when retention_month = 28 then cast(retention_percentage as double) end) as M28
        -- ,
    
        -- sum(case when retention_month = 29 then cast(retention_percentage as double) end) as M29
        -- ,
    
        -- sum(case when retention_month = 30 then cast(retention_percentage as double) end) as M30
        -- ,
    
        -- sum(case when retention_month = 31 then cast(retention_percentage as double) end) as M31
        -- ,
    
        -- sum(case when retention_month = 32 then cast(retention_percentage as double) end) as M32
        -- ,
    
        -- sum(case when retention_month = 33 then cast(retention_percentage as double) end) as M33
        -- ,
    
        -- sum(case when retention_month = 34 then cast(retention_percentage as double) end) as M34
        -- ,
    
        -- sum(case when retention_month = 35 then cast(retention_percentage as double) end) as M35
        -- ,
        
        -- sum(case when retention_month = 36 then cast(retention_percentage as double) end) as M36
        
    
from returning_cohorts
group by 1,2,3
order by 1 desc

),

final as (

    select * from retention_percentage

)

select * from final
order by 1 desc
