with dess as (
    select * from {{ref("stg_desarrolladores")}}
), paiss as (
    select * from {{ref("stg_paises")}}
)

select 
    desarrollador_id,
    desarrollador,
    ciudad,
    division_administrativa,
    d.pais,
    continente,
    fundacion,
    juegos_notables,
    notas

from dess d 
join paiss p 
on d.pais = p.country