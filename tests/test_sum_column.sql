--Test para comprobar que la suma total coincide 
select * 
from {{source("trabajo_final",'ventas')}} 
where ventas_na + ventas_eu + ventas_jp + otras_ventas != ventas_globales