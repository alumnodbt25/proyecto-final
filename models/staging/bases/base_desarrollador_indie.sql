with des_indie as (
    select 
    cast(lower(desarrollador) as varchar(256)) as desarrollador,
    cast(lower(ciudad) as varchar(256)) as ciudad,
    cast(lower(autonomous_area) as varchar(256)) as division_administrativa,
    cast(lower(pais) as varchar(256)) as pais,
    cast(juegos_notables as varchar(450)) as juegos_notables,
    cast(notas as varchar(300)) as notas
     from {{source('trabajo_final','desarrollador_indie')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['desarrollador']) }} as desarrollador_id,
    desarrollador,
    ciudad,
    division_administrativa,
    pais,
    NULL as fundacion,
    juegos_notables,
    notas
    from des_indie