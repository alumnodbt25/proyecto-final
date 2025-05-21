with cte_1 as (
    with estudio_empresas as (
    select 
        desarrollador,
        pais
    from {{ref('dim_desarrolladores')}}
), est as (
    select 
        empresa,
        ventas_globales,
        ganancias_dolares
    from {{ref("fct_ventas")}}
)

select 
    cantidad_empresas,
    pais


from (

    select 
        count (desarrollador) over (partition by pais) as cantidad_empresas,
        ROW_NUMBER() OVER (PARTITION BY pais ORDER BY pais DESC) AS row_num,
        pais
        from estudio_empresas
     order by row_num
), 

where row_num = 1), cte_2 as (
    with ctee as (
select 
    empresa,
    ventas_totales,
    ganancias

    from (
    select 
    sum(ventas_globales) over (partition by empresa) as ventas_totales,
    sum(ganancias_dolares) over (partition by empresa) as ganancias,
    empresa,
    row_number() over (partition by empresa order by empresa) as roww

from ALUMNO25_DEV_GOLD_DB.DBT_NCANO.FCT_VENTAS)
where roww = 1), cte2 as (
    select 
        desarrollador,
        pais
    from ALUMNO25_DEV_GOLD_DB.DBT_NCANO.DIM_DESARROLLADORES
)

select 
    empresa,
    ventas_totales,
    ganancias,
    pais
    
from (
    select 
    empresa,
    ventas_totales,
    ganancias,
    pais,
    row_number() over (partition by pais order by ventas_totales desc) as roww2
    from ctee e 
    join cte2 r
    on e.empresa = r.desarrollador
    order by pais,ventas_totales,roww2)
    where roww2 = 1
)

select 
    a.pais,
    cantidad_empresas,
    empresa,
    ventas_totales,
    ganancias
from cte_1 a 
left join cte_2 b 
on a.pais = b.pais

