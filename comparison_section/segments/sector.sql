select 
    protocol,
    type as segment
from dune.pyor_xyz.dataset_arb_incentives_distributed

set fallback_value = 'other'