

with 
user_segmentation as (
    select 
        user,
        segment
    from query_{{user_segment_query}}
),
protocol_segmentation as (
    select 
        protocol,
        segment
    from query_{{protocol_segment_query}}
),
first_occurence_eoa as (
    select  
        s.user as "from",
        coalesce(p.segment, {{protocol_fallback_value}}) as protocol_segment,
        coalesce(u.segment, {{user_fallback_value}}) as segment,
        min(date) as fod
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
        left join segmentation u 
            on s.user = u.user
        left join protocol_segmentation p 
            on s.protocol = p.protocol
    where 1=1
    -- and protocol = 'Camelot'
    group by 1,2,3
),
new_users_eoa as (
    select        
        fod as date,
        protocol_segment,
        segment,
        count(distinct "from") as new_eoa_addresses
    from first_occurence_eoa
    group by 1,2,3
),
total_addresses_eoa as (
    select 
        date,
        segment,
        protocol_segment,
        new_eoa_addresses,
        sum(new_eoa_addresses) over (partition by protocol_segment, segment order by date) as total_eoa_cumulative_addresses
    from new_users_eoa
)

select * from total_addresses_eoa

