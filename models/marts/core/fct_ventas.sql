with ventass as (
    select * from {{ref("stg_ventas")}}
), juegos as (
    select * from {{ref("int_juegos")}}
), consolass as (
    select * from {{ref("stg_consolas")}}
), emp as (
    select * from {{ref("stg_desarrolladores")}}
), inf as (
    select * from {{ref("stg_inflacion")}}
), inter as (

select 
    ventas_id,
    juego_id,
    v.videojuego,
    c.consola_id,
    v.consola,
    e.desarrollador_id,
    v.empresa,
    v.lanzamiento,
    precio_lanzamiento as precio_dolares,
    precio_dolares * cambio_dolar_euro as precio_dolares_euros,
    NULL as precio_euros,
    precio_euros * cambio_euro_dolar as precio_euros_dolares,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    v.ventas_globales,
    critica

from ventass v
join juegos j
on v.videojuego = j.videojuego
join consolass c
on v.consola = c.abreviatura
left join emp e 
on e.desarrollador = v.empresa
join inf i 
on i.anyo = v.lanzamiento
order by precio_dolares
)

select 
  ventas_id,
    juego_id,
    videojuego,
    consola_id,
    consola,
    desarrollador_id,
    empresa,
    lanzamiento,
    case 
        when precio_dolares IS NULL THEN precio_euros_dolares
        else precio_dolares
        end as precio_dolares,
    case 
        when precio_euros IS NULL then precio_dolares_euros
        else precio_euros 
        end as precio_euros,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    ventas_globales,
    critica
from inter
order by precio_dolares