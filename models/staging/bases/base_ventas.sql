with ventas as (
    select 
    lower(nombre) as videojuego,
    lower(plataforma) as consola,
    year_published as lanzamiento,
    lower(genero) as genero,
    lower(publisher) as empresa,
    precio_dolares,
    precio_euros,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    ventas_globales,
    reviews as critica
    
     from {{source('trabajo_final','ventas')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['videojuego','ventas_globales']) }} as ventas_id,
    videojuego,
    consola,
    lanzamiento,
    genero,
    empresa,
    precio_dolares,
    precio_euros,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    ventas_globales,
    critica

from ventas