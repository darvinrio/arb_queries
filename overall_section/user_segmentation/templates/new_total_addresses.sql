

with 
segmentation as (
    select 
        user,
        segment
    from query_{{segment_query}}
),
first_occurence_eoa as (
    select  
        s.user as "from",
        coalesce(u.segment, {{fallback_value}}) as segment,
        min(date) as fod
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
        left join segmentation u 
            on s.user = u.user
    where 1=1
    group by 1,2
),
new_users_eoa as (
    select        
        fod as date,
        segment,
        count(distinct "from") as new_eoa_addresses
    from first_occurence_eoa
    group by 1,2
),
total_addresses_eoa as (
    select 
        date,
        segment,
        new_eoa_addresses,
        sum(new_eoa_addresses) over (partition by  segment order by date) as total_eoa_cumulative_addresses
    from new_users_eoa
)

select * from total_addresses_eoa

