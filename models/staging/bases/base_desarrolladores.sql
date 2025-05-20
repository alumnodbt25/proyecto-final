with desarrollador as (
    select 
    lower(desarrollador) as desarrollador,
    lower(ciudad) as ciudad,
    lower(division_administrativa) as division_administrativa,
    lower(pais) as pais,
    year_published as fundacion,
    juegos_notables,
    notas
    
     from {{source('trabajo_final','desarrolladores')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['desarrollador']) }} as desarrollador_id,
    desarrollador,
    ciudad,
    division_administrativa,
    pais,
    fundacion,
    juegos_notables,
    notas
    
from desarrollador