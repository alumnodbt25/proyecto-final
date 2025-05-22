with consolas as (
    select 
        cast(lower(consola) as varchar(256)) as consola,
        cast(lower(abreviatura) as varchar(256)) as abreviatura,
        cast(lower(nombre_2) as varchar(256)) as nombre_2,
        cast(lanzamiento as integer) as lanzamiento,
        cast(lower(empresa) as varchar(256)) as empresa,
        cast(replace(precio_na,',','.') as decimal(38,2)) as precio_na,
        cast(replace(ventas_globales,',','.') as decimal(38,2)) as ventas_globales,
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