-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### El área de Demand Planning requiere información con mayor frecuencia de las transacciones de venta y movimientos de mercadería realizadas. Para ello genera reportes  comerciales  a  través  del  área  de Reporting  con  diversos  KPIs,  indicadores segmentando por distintas categorías
-- MAGIC
-- MAGIC ##### [Link al modelo de datos](https://drive.google.com/file/d/1NNaDbxox1cj3vgA6THKbSNgf6pLe7ES-/view)
-- MAGIC
-- MAGIC ###### El modelo cuenta con las siguientes tablas:
-- MAGIC - Clientes: Listado de los clientes dados de alta en el sistema de ventas.
-- MAGIC - Empleados: Maestro de empleados, el mismo esta compuesto por el identificador, nombre, apellido y sucursal en la que trabaja.
-- MAGIC - Locales: Maestro de sucursales compuesta por el identificador, nombre y tipo de local.
-- MAGIC - Productos: Maestro de productos con su precio agrupados por familia de producto.
-- MAGIC - Facturas: Tabla que registra todas las transacciones (ventas). Además contiene, la fecha de en que se realizó la operación, el empleado que hizo la venta, el cliente y la cantidad de productos vendidos
-- MAGIC
-- MAGIC INTEGRANTES:
-- MAGIC   ROCIO, BAUR

-- COMMAND ----------

USE CATALOG curso;
USE SCHEMA ventas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Generar un listado de la cantidad de productos vendidos por año de manera descendente.

-- COMMAND ----------

SELECT 
YEAR(fecha_venta) AS `Año_venta`,
SUM(cantidad) AS `Total_productos_vendidos`
FROM curso.ventas.facturas
GROUP BY `Año_venta`
ORDER BY `Año_venta` DESC;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Top-5 de los empleados que menos vendieron según cantidad vendida, indicando apellido y nombre en un sólo campo. 
-- MAGIC
-- MAGIC **_Los 5 empleados que menos vendieron fueron Jose Pérez, Maria González, Luis Fernández, Ramon González y Hector._**

-- COMMAND ----------

-- DBTITLE 1,Untitled
SELECT 
  CONCAT_WS (' ', e.nombre, CASE WHEN e.apellido = 'null' THEN  '' ELSE e.apellido END) AS Nombre_completo_empleado,
  SUM(f.cantidad) AS cantidad_vendida   
FROM curso.ventas.facturas AS f
JOIN curso.ventas.empleados AS e
  ON f.vendedor = e.id_vendedor
GROUP BY id_vendedor, CONCAT_WS ( ' ', e.nombre, CASE WHEN e.apellido = 'null' THEN  '' ELSE e.apellido END) 
ORDER BY `cantidad_vendida` ASC
LIMIT 5;



-- COMMAND ----------

SELECT * FROM curso.ventas.facturas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  3. ¿Cuántos clientes compraron mes anterior ?
-- MAGIC
-- MAGIC **En el mes anterior (Agosto 2023) fueron 1677 clientes los que compraron.**
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _Al no haber una fecha de referencia en la consigna, se toma como 'mes actual' el último periodo registrado en la tabla de facturas (2023-09-17), mes de Septiembre. Por lo tanto, el cálculo se realiza sobre el mes de agosto._

-- COMMAND ----------

--calculo mes anterior--
select * from curso.ventas.facturas
order by fecha_venta DESC
LIMIT 1

-- COMMAND ----------

-- DBTITLE 1,Untitled
SELECT 
  COUNT (DISTINCT cliente) AS `Total_clientes`
FROM curso.ventas.facturas 
WHERE MONTH (fecha_venta) = 08
AND YEAR (fecha_venta) = 2023;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. ¿Cuál fue el producto que se vendió mas en el año 2022? ¿A qué familia de producto pertenece? -
-- MAGIC
-- MAGIC **_El producto que se vendió más en el año 2022 fue el Triángulo, de la familia de Chocolates, alcanzando un volumen total de 57,911 unidades._**

-- COMMAND ----------

SELECT
  nombre, 
  familia,
  SUM (f.cantidad) AS `Total_ventas`
FROM curso.ventas.facturas AS f
JOIN curso.ventas.productos AS p
  ON f.producto = p.id_producto
WHERE YEAR (f.fecha_venta) = 2022
GROUP BY p.nombre, p.familia
ORDER BY total_ventas DESC
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Siguiendo con el punto anterior ¿Y cuál fue el más rentable?
-- MAGIC
-- MAGIC **_El producto más rentable del 2022 fue Häagen-Dazs, con una recaudación total de $7.978.518. Al no contar con información sobre los costos unitarios, el cálculo se basa en el ingreso total generado por las ventas durante dicho periodo._**

-- COMMAND ----------

SELECT
   p.nombre,
  SUM (cantidad * precio_unitario) AS recaudacion_total
FROM curso.ventas.facturas AS f
JOIN curso.ventas.productos AS p
  ON f.producto = p.id_producto
WHERE YEAR (f.fecha_venta) = 2022
GROUP BY   f.producto,  p.nombre
ORDER BY recaudacion_total DESC
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Top-10 de sucursales según monto vendido, indicando el monto, ordenado de mayor a menor. El informe debe mostrar:
-- MAGIC - Tipo de local
-- MAGIC - Nombre del local
-- MAGIC - Monto vendido
-- MAGIC
-- MAGIC **_Las sucursales tipo Supermercado lideran la recaudación, representando la mayoría del Top 10. El local Éxito La 33 es el punto de mayor impacto en las ventas totales._**

-- COMMAND ----------

SELECT 
tipo AS tipo_local, 
l.nombre,
SUM (f.cantidad*p.precio_unitario) AS monto_vendido
FROM curso.ventas.productos AS p 
JOIN curso.ventas.facturas AS f
  ON p.id_producto = f.producto
JOIN curso.ventas.empleados AS e
  ON f.vendedor = e.id_vendedor
JOIN curso.ventas.locales AS l
  ON  e.sucursal = l.id_sucursal
GROUP BY l.nombre, tipo
ORDER BY monto_vendido DESC
LIMIT 10;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Se detectaron ventas (facturas) realizadas por vendedores que no estan mas en la compañia (no estan en el maestro de empleados). Por lo tanto, nos solicitan un listado de dichos empleados con la cantidad de ventas (facturas). ¿Cuántos empleados son?
-- MAGIC
-- MAGIC **_Los vendedores identificados con los IDs del 57 al 65,  no registran datos de Nombre o Apellido, ya que no se encuentran en la tabla maestra de empleados. Esto confirma que son registros de ventas de personal que ya no pertenece a la compañía. Son un total de 9 empleados._**

-- COMMAND ----------

-- DBTITLE 0,Untitled
SELECT 
  f.vendedor AS id_venderdor,
  COUNT (*) AS cantidad_ventas
FROM curso.ventas.facturas AS f
LEFT JOIN curso.ventas.empleados AS e
  ON f.vendedor = e.id_vendedor
WHERE e.id_vendedor IS NULL
GROUP BY f.vendedor
ORDER BY cantidad_ventas DESC

-- COMMAND ----------

SELECT 
   COUNT (DISTINCT f.vendedor) AS total_ex_empleados
FROM curso.ventas.facturas AS f 
LEFT JOIN curso.ventas.empleados AS e
  ON f.vendedor = e.id_vendedor
WHERE e.id_vendedor IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Nos piden clasificar a los vendedores en funcion de su rendimiento (facturación) para el año actual.
-- MAGIC - "Excelente" si el vendedor ha vendido por más de 10 millones de pesos en total.
-- MAGIC - "Bueno" si el vendedor ha vendido entre 5 y 10 millones de pesos en total.
-- MAGIC - "Regular" si el vendedor ha vendido menos de 5 millones de pesos en total.
-- MAGIC
-- MAGIC **_Se toma el 2023 como "año actual" por ser el último con registros completos. 
-- MAGIC Se observa que ningún vendedor alcanzó la categoría "Excelente" (>$10M) en este periodo. Esto sugiere un desplazamiento de la fuerza de ventas hacia los rangos "Bueno" y "Regular", lo que podría deberse a una contracción del mercado o a cambios en el equipo comercial respecto a años anteriores._**

-- COMMAND ----------

-- DBTITLE 1,Untitled
SELECT  
  e.id_vendedor,
  SUM(p.precio_unitario * f.cantidad) AS total_ventas,
  CASE 
    WHEN SUM (p.precio_unitario * f.cantidad) >= 10000000 THEN 'Excelente'
    WHEN SUM (p.precio_unitario * f.cantidad) >= 5000000 THEN 'Bueno'
    ELSE 'Regular'
END AS rendimiento
FROM curso.ventas.productos AS p
JOIN curso.ventas.facturas AS f
  ON p.id_producto = f.producto 
JOIN curso.ventas.empleados AS e
  ON f.vendedor = e.id_vendedor 
WHERE YEAR (fecha_venta) = 2023
GROUP BY e.id_vendedor
ORDER BY e.id_vendedor ASC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Muestra el número total de facturas para cada vendedor que haya realizado más de 100 ventas el año anterior. Incluye el nombre del vendedor y la cantidad de facturas.

-- COMMAND ----------

-- DBTITLE 1,Cell 24
SELECT 
  CONCAT_WS (' ', e.nombre, CASE WHEN e.apellido = 'null' THEN  '' ELSE e.apellido END) AS Nombre_empleado,
  COUNT(*) AS cantidad_facturas 
FROM curso.ventas.facturas AS f
JOIN curso.ventas.empleados AS e
  ON f.vendedor = e.id_vendedor
WHERE YEAR(f.fecha_venta) = 2022
GROUP BY id_vendedor,e.nombre, e.apellido
HAVING COUNT(*) >= 100
ORDER BY cantidad_facturas DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Generar un listado de los clientes que realizaron mas de 50 compras y que su edad sea mayor al premedio de edad del total de nuestra base de clientes. Ordenar el listado por edad de manera ascendente
-- MAGIC
-- MAGIC **_La edad promedio de los clientes es de 47 años._**

-- COMMAND ----------

-- DBTITLE 1,Untitled
SELECT 
  CONCAT(nombre, ' ' , apellido) AS cliente,
  (2023 - YEAR(fecha_nacimiento)) AS edad,
  COUNT(*) AS total_compras
FROM curso.ventas.clientes AS c
JOIN curso.ventas.facturas AS f
  ON c.id_cliente = f.cliente
WHERE (2023 - YEAR (fecha_nacimiento))>= 
(SELECT AVG(2023 - YEAR (fecha_nacimiento))
FROM curso.ventas.clientes) 
GROUP BY c.nombre, c.apellido, c.fecha_nacimiento
HAVING COUNT(*) >= 50 
ORDER BY edad ASC



-- COMMAND ----------

---Cálculo Edad Promedio Clientes ---

SELECT 
  CAST(AVG(2023 - YEAR(fecha_nacimiento)) AS INT) AS edad_promedio
FROM curso.ventas.clientes;