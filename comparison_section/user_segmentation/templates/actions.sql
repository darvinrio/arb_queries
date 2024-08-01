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
data as (
    select 
        date,
        s.user,
        coalesce(p.segment, {{protocol_fallback_value}}) as protocol_segment,
        coalesce(u.segment, {{user_fallback_value}}) as segment,
        count(*) as actions
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
        left join user_segmentation u 
            on s.user = u.user
        left join protocol_segmentation p 
            on s.protocol = p.protocol
    -- where protocol = 'Camelot'
    group by 1,2,3,4
),
aggr as (
    select 
        date,
        protocol_segment,
        segment,
        avg(actions) as avg_actions_per_user,
        approx_percentile(actions, 0.5) as median_actions_per_user
    from data
    group by 1,2,3
)

select *,
    avg(avg_actions_per_user) over(partition by protocol_segment,segment order by date rows between 7 preceding and current row) as avg_actions_per_user_7ma
from aggr
