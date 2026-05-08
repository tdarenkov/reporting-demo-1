select dim_key, dim_description, source_file, updated_at
from {{ source('bronze_hls', 'gl_dim_catalogs') }}
