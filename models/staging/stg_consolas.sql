with consolas as (
    select 
        lower(consola) as consola,
        lower(abreviatura) as abreviatura,
        lower(nombre_2) as nombre_2,
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
    nombre_2,
    lanzamiento,
    empresa,
    precio_na,
    ventas_globales
from consolas