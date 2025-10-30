from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('covid-19').getOrCreate()

# Ruta del archivo en HDFS
file_path = 'hdfs://localhost:9000/covid-19/gt2j-8ykr.csv'

# Cargar el archivo CSV
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

print("=== Esquema del DataFrame ===")
df.printSchema()

# Ver las primeras columnas para confirmar nombres
print("=== Columnas del DataFrame ===")
print(df.columns)

# ----------------------------
# LIMPIEZA DE DATOS
# ----------------------------

# Verificar nulos
print("=== Valores nulos por columna ===")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Eliminar duplicados
df = df.dropDuplicates()

# Eliminar filas sin edad o sin departamento
df = df.dropna(subset=['edad', 'departamento_nom'])

# ----------------------------
# TRANSFORMACIÓN DE DATOS
# ----------------------------

# Estandarizar texto
df = df.withColumn('sexo', F.upper(F.col('sexo')))

# ----------------------------
# ANÁLISIS EXPLORATORIO DE DATOS
# ----------------------------

print("=== Estadísticas básicas de la edad ===")
df.select('edad').summary().show()

# Consulta 1: Mayores de 50 años
print("=== Casos mayores de 50 años ===")
mayores_50 = df.filter(F.col('edad') > 50).select('id_de_caso', 'edad', 'sexo', 'departamento_nom', 'estado')
mayores_50.show(10)

# Consulta 2: Ordenar por fecha de reporte
print("=== Casos ordenados por fecha de reporte ===")
fecha_ordenados = df.sort(F.col('fecha_reporte_web').desc())
fecha_ordenados.show(10)

# Consulta 3: Casos por departamento
print("=== Casos por departamento ===")
casos_departamento = df.groupBy('departamento_nom').count().sort(F.col('count').desc())
casos_departamento.show(10)

# ----------------------------
#  GUARDAR RESULTADOS
# ----------------------------

print("=== Guardando resultados en HDFS ===")
df.write.mode("overwrite").csv("hdfs://localhost:9000/covid-19/resultado_procesado")

print("=== Proceso finalizado con éxito ===")

spark.stop()
