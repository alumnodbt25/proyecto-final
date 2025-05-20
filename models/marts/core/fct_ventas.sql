with ventass as (
    select * from {{ref("stg_ventas")}}
), juegos as (
    select * from {{ref("dim_juegos")}}
), consolass as (
    select * from {{ref("dim_consolas")}}
), emp as (
    select * from {{ref("stg_desarrolladores")}}
), inf as (
    select * from {{ref("dim_inflacion")}}
), prodd as (
    select * from {{ref("stg_productos")}}
), inter as (

select 
    ventas_id,
    juego_id,
    v.videojuego,
    c.consola_id,
    v.consola,
    ROW_NUMBER() OVER(PARTITION BY v.videojuego,v.consola ORDER BY v.videojuego ASC) as dup,
    e.desarrollador_id,
    v.empresa,
    v.lanzamiento,
    p.precio_lanzamiento as precio_dolares,
    precio_dolares * cambio_dolar_euro as precio_euros,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    v.ventas_globales,
    v.critica

from ventass v
 join juegos j
on v.videojuego = j.videojuego 
left join consolass c
on v.consola = c.abreviatura
 join emp e 
on e.desarrollador = v.empresa
 join inf i 
on i.fecha = v.lanzamiento
left join prodd p
on v.videojuego = p.videojuego
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
    precio_dolares,
    precio_euros,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    ventas_globales,
    critica
from inter
where dup = 1
order by videojuego
