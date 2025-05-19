--Dimensión de la inflación global y el cambio de monedas
with dim_inf as (
    select * from {{ref("stg_inflacion")}}
)

select 
    inflacion_id,
    anyo as fecha, 
    inflacion_global,
    lag(inflacion_global) OVER (ORDER BY fecha) as fecha_anterior, --poder ver la diferencia con el año anterior
    inflacion_global - LAG(inflacion_global) OVER (ORDER BY fecha) AS incremento, --para poder analizar subidas y bajadas 
    cambio_euro_dolar,
    cambio_dolar_euro

from dim_inf,
order by fecha DESC