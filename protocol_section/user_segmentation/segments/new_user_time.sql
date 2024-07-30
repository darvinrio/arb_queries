
select 
    user,
    case 
        when first_appearance <  date'2023-11-01' then 'pre incentives'
        when first_appearance <  date'2024-01-01' then 'stip new'
        when first_appearance <  date'2024-06-01' then 'backfund new'
        else 'ltipp new'
    end as segment
from dune.pyor_xyz.result_arb_user_summary

set fallback_value = 'pre incentives'