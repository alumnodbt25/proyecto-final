--Dimensión de las consolas y sus ventas
with cons as (
    select * from {{ref("stg_consolas")}}
)

select 
    consola_id,
    consola,
    abreviatura,
    nombre_2,
    lanzamiento,
    empresa,
    precio_na,
    ventas_globales,
    precio_na * ventas_globales as ganancias_globales --ganancias globales para luego poder hacer análisis
 
from cons