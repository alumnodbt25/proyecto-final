with desar as (
    select * from {{ref("int_desarrolladores")}}
)

select 
    desarrollador_id, 
    desarrollador,
    ciudad,
    case when division_administrativa IS NULL then 'Sin especificar' 
    else division_administrativa
    end as division_administrativa,
    pais,
    continente,
    fundacion,
    case when juegos_notables IS NULL then 'Desconocido'
    else juegos_notables end as juegos_notables,
    case when notas IS NULL then 'Sin Notas'
    else notas end as notas

from desar 