-- macros/calcular_precio_ajustado.sql

{% macro calcular_precio_ajustado(tabla_productos, tabla_factores, columna_producto_id, columna_precio_original, columna_anio, columna_fecha, columna_factor_multiplicacion) %}
    WITH RECURSIVE multiplicaciones AS (
        -- Fila inicial con el precio base y el primer factor
        SELECT
            p.{{ columna_producto_id }},
            p.{{ columna_precio_original }},
            f.{{ columna_anio }},
            p.{{columna_fecha}},
            f.{{ columna_factor_multiplicacion }},
            p.{{ columna_precio_original }} + p.{{ columna_precio_original }} * (f.{{ columna_factor_multiplicacion }}/100) AS precio_calculado,
            1 AS row_num
        FROM {{ ref(tabla_productos) }} p
        JOIN {{ ref(tabla_factores) }} f
            ON f.{{ columna_anio}} = p.{{columna_fecha}} + 1

        UNION ALL

        -- Repetir multiplicación con el siguiente factor de la tabla
        SELECT
            m.{{ columna_producto_id }},
            m.{{ columna_precio_original }},
            f.{{ columna_anio }},
            m.{{columna_fecha}},
            f.{{ columna_factor_multiplicacion }},
            m.precio_calculado + m.precio_calculado * (f.{{ columna_factor_multiplicacion }}/100) AS precio_calculado,
            m.row_num + 1 AS row_num  -- Aumentamos el contador de filas
        FROM multiplicaciones m
        JOIN {{ ref(tabla_factores) }} f
            ON f.{{ columna_anio }} = m.{{ columna_anio }} + 1  -- Avanzamos al siguiente año
    )

    -- Seleccionamos solo el último año de cada producto usando ROW_NUMBER
    SELECT
        {{ columna_producto_id }},
        {{ columna_precio_original }},
        {{columna_fecha}},
        fecha,
        precio_calculado,
        round(precio_calculado/{{columna_precio_original}}*100, 2) || '%' as porcentaje_incremento
    FROM (
        SELECT
            {{ columna_producto_id }},
            {{ columna_precio_original }},
            {{columna_fecha}},
            fecha,
            precio_calculado,
            ROW_NUMBER() OVER (PARTITION BY {{ columna_producto_id }} ORDER BY fecha DESC) AS row_num
        FROM multiplicaciones
    ) AS resultado
    WHERE row_num = 1 and {{columna_precio_original}} IS NOT NULL -- Solo el último año de cada producto
    ORDER BY precio_calculado desc
{% endmacro %}

