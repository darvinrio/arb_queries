with 
segmentation as (
    select 
        protocol,
        segment
    from query_{{segment_query}}
),
data as (
    select 
        date,
        s.user,
        coalesce(u.segment, {{fallback_value}}) as segment,
        count(*) as actions
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
        left join segmentation u 
            on s.protocol = u.protocol
    -- where protocol = 'Camelot'
    group by 1,2,3
),
aggr as (
    select 
        date,
        segment,
        avg(actions) as avg_actions_per_user,
        approx_percentile(actions, 0.5) as median_actions_per_user
    from data
    group by 1,2
)

select *,
    avg(avg_actions_per_user) over(partition by segment order by date rows between 7 preceding and current row) as avg_actions_per_user_7ma
from aggr
