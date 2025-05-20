with ventas as (
    select 
    lower(nombre) as videojuego,
    lower(plataforma) as consola,
    year_published as lanzamiento,
    lower(genero) as genero,
    lower(publisher) as empresa,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    reviews as critica
    
     from {{source('trabajo_final','ventas')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['videojuego','consola']) }} as ventas_id,
    {{ dbt_utils.generate_surrogate_key(['videojuego']) }} as juego_id,
    videojuego,
    consola,
    {{ dbt_utils.generate_surrogate_key(['empresa']) }} as empresa_id,
    empresa,
    lanzamiento,
    genero,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    critica

from ventas