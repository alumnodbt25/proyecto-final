with consolas as (
    select 
        lower(consola) as consola,
        lower(abreviatura) as abreviatura,
        lanzamiento,
        lower(empresa) as empresa,
        precio_na,
        ventas_globales
        from {{source('trabajo_final','consolas')}}
) 

select 
    {{ dbt_utils.generate_surrogate_key(['consola']) }} as consola_id,
    consola,
    abreviatura,
    lanzamiento,
    empresa,
    precio_na,
    ventas_globales
from consolas