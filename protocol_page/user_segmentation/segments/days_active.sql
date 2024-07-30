
select 
    user,
    case 
        when days_active <= 31 then 'one month'
        when days_active <= 120 then '4 months'
        when days_active <= 300 then '10 months'
        when days_active <= 440 then '14 months'
        else 'more than a year'
    end as segment
from dune.pyor_xyz.result_arb_user_summary

set fallback_value = 'one month'