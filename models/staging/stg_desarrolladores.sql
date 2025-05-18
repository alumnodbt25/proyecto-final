
with des as (
    select * from {{ref("base_desarrolladores")}}
), des_ind as (
    select * from {{ref("base_desarrollador_indie")}}
), unite as (
    select * from des
    union 
    select * from des_ind
), not_dup as (
    select 
        desarrollador_id,
        desarrollador,
        ciudad,
        division_administrativa,
        pais,
        fundacion,
        juegos_notables,
        notas,
        ROW_NUMBER() OVER(PARTITION BY desarrollador_id ORDER BY desarrollador ASC) as dup
    from unite,
    order by desarrollador
)

select 
    desarrollador_id,
    desarrollador,
    ciudad,
    division_administrativa,
    pais,
    fundacion,
    juegos_notables,
    notas
from not_dup where dup = 1