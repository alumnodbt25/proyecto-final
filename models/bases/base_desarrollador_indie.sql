with des_indie as (
    select * from {{source('trabajo_final','desarrollador_indie')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['desarrollador']) }} as desarrollador_id,
    lower(desarrollador) as desarrollador,
    lower(ciudad) as ciudad,
    lower(autonomous_area) as division_administrativa,
    lower(pais) as pais,
    'Desconocido' as fundacion,
    juegos_notables,
    notas
    from des_indie