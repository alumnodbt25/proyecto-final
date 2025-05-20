with ventass as (
    select * from {{ref("int_ventas")}}
), inf as (
    select * from {{ref("dim_inflacion")}}
) 

select 
    ventas_id,
    juego_id,
    v.videojuego,
    consola_id,
    v.consola,
    empresa_id,
    v.empresa,
    v.lanzamiento,
    precio_dolares,
    precio_dolares * cambio_dolar_euro as precio_euros,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    v.ventas_globales,
    v.critica

from ventass v
join inf i 
on i.fecha = v.lanzamiento
order by videojuego


