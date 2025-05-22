with desarrollador as (
    select 
    cast(lower(desarrollador) as varchar(256)) as desarrollador,
    cast(lower(ciudad) as varchar(256)) as ciudad,
    cast(lower(division_administrativa) as varchar(256)) as division_administrativa,
    cast(lower(pais) as varchar(256)) as pais,
    cast(year_published as integer) as fundacion,
    cast(juegos_notables as varchar(600)) as juegos_notables,
    cast(notas as varchar(256)) as notas
    
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