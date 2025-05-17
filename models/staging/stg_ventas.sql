with ventass as (
    select * from {{ref("base_ventas")}}
), juegos as (
    select * from {{ref("stg_juegos")}}
)

select 
    ventas_id,
    juego_id,
    v.videojuego,
    consola,
    lanzamiento,
    precio_dolares,
    precio_euros,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    ventas_globales,
    critica

from ventass v
join juegos j
on v.videojuego = j.videojuego