from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import re
from datetime import datetime



def read_data(spark, file_path):
    """
    Read data from a json file into a Spark DataFrame.
    """
    df = spark.read.option("multiline", "true").json(file_path)

    return df


def get_parsed_information(df, data_name):
    """
    Extract and return the 'citas_medicas' or 'pacientes' field from the DataFrame.
    """
    df_pacientes = df.select(F.explode(F.col(data_name)).alias(data_name))
    return df_pacientes.select(f"{data_name}.*")


def parse_spanish_date(text: str):
    """
    Devuelve fecha en formato ISO 'YYYY-MM-DD' o None si no se puede parsear.
    Soporta:
      - 'YYYY-MM-DD' (ISO)
      - '14 de diciembre de 2007'
      - '22 de oct de 2002'
    """
    SPANISH_MONTHS = {
        # completos
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
        # abreviados (más frecuentes en español latino)
        "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
        "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    }
    ISO_DATE_RE = re.compile(r"^\s*\d{4}-\d{2}-\d{2}\s*$")
    SPANISH_LONG_RE = re.compile(
        r"^\s*(\d{1,2})\s+de\s+([A-Za-záéíóúñÑ\.]{3,})\s+de\s+(\d{4})\s*$",
        re.IGNORECASE,
    )

    if text is None:
        return None
    s = str(text).strip()

    # ISO directo
    if ISO_DATE_RE.match(s):
        try:
            # Validamos que sea una fecha real
            dt = datetime.strptime(s, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    # Español largo
    m = SPANISH_LONG_RE.match(s.lower().replace(".", ""))  # quitar puntos de abreviaturas
    if m:
        day_s, month_s, year_s = m.groups()
        try:
            day = int(day_s)
            month = SPANISH_MONTHS.get(month_s, None)
            year = int(year_s)
            if month is None:
                return None
            dt = datetime(year, month, day)  # esto levanta ValueError si es inválida (p.ej., día 33)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    return None


def compute_age_from_birthdate_col(col_date):
    """Edad entera a partir de fecha de nacimiento (en años)."""
    return F.floor(F.months_between(F.current_date(), col_date) / F.lit(12.0)).cast("int")


def clean_date(df, date_col):
    """
    Clean and standardize date columns to 'yyyy-MM-dd' format.
    """
    parse_udf = F.udf(parse_spanish_date, T.StringType())
    df = df.withColumn(date_col, parse_udf(F.col(date_col)))
    df = df.withColumn(date_col, F.to_date(F.col(date_col), "yyyy-MM-dd"))
    return df


def calculate_age(df, col_age, col_date_validation):
    # 3) Edad calculada y conciliación con 'edad'
    df = df.withColumn(col_age, compute_age_from_birthdate_col(F.col(col_date_validation)))
    # regla: si edad existe y |edad - edad_calc| <= 2, mantener; si no, usar edad_calc
    df = df.withColumn(
        col_age,
        F.when(
            (F.col(col_age).isNotNull()) & (F.abs(F.col(col_age) - F.col(col_age)) <= 2),
            F.col(col_age).cast("int")
        ).otherwise(F.col(col_age))
    )


    return df


def clean_gender(df, column):
    """
    Clean the 'sexo' field to standardize its values.
    """
    df = df.withColumn(column, F.lower(F.col(column)))
    df = df.withColumn(column, F.when(F.col(column).isin("m", "male"), "M")
                             .when(F.col(column).isin("f", "female"), "F")
                             .otherwise(None))
    return df


def clean_email(df, col_email):
    """
    Clean the 'email' field to ensure valid email formats.
    """
    EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    df = df.withColumn(col_email, F.when(F.col(col_email).rlike(EMAIL_REGEX), F.col(col_email)).otherwise(None))
    return df


def clean_phone(df, col_phone):
    """
    Clean the 'telefono' field to ensure valid phone number formats.
    """

    df = df.withColumn(col_phone, F.regexp_replace(F.coalesce(F.col(col_phone), F.lit("")), r"\D+", ""))
    return df


def drop_duplicates(df, subset_cols):
    """
    Drop duplicate rows based on a subset of columns.
    """
    df = df.dropDuplicates(subset=subset_cols)
    df = df.distinct()
    return df


def apply_transformations_to_pacientes(df_pacientes):
    """
    Apply necessary transformations to the 'pacientes' DataFrame.
    """

    df_pacientes = clean_date(df_pacientes, 'fecha_nacimiento') #Clean and standardize date
    df_pacientes = calculate_age(df_pacientes, 'edad', 'fecha_nacimiento') #clean and calculate age if we want to keep the column
    df_pacientes = clean_gender(df_pacientes, 'sexo') #Clean Gender
    df_pacientes = clean_email(df_pacientes, 'email') #Clean email
    df_pacientes = clean_phone(df_pacientes,'telefono') #Clean phone number  
    df_pacientes = drop_duplicates(df_pacientes, subset_cols=['id_paciente','fecha_nacimiento']) #drop duplicates based on id and birthdate

    return df_pacientes


def apply_transformations_to_citas_medicas(df_citas_medicas):
    """
    Apply necessary transformations to the 'citas_medicas' DataFrame.
    """

    df_citas_medicas = clean_date(df_citas_medicas, 'fecha_cita') #Clean and standardize date
    df_citas_medicas = drop_duplicates(df_citas_medicas, subset_cols=['id_paciente', 'fecha_cita', 'especialidad']) #drop duplicates based on id, date and specialty
    df_citas_medicas = df_citas_medicas.join(df_pacientes, on='id_paciente', how='inner') #Keep only records with valid patient id  
    df_citas_medicas = df_citas_medicas.select(
        F.col('id_cita'),
        F.col('id_paciente'),
        F.col('fecha_cita'),
        F.col('especialidad'),
        F.col('medico'),
        F.col('costo'),
        F.col('estado_cita'),
    ) #Select only relevant columns after join
    
    return df_citas_medicas  
    


#========================================= START PIPELINE ==================================================

if __name__ == "__main__":
    spark = SparkSession.builder.appName('xpert_group_etl').getOrCreate()

    #1. Extract
    df = read_data(spark, "data/dataset_hospital 2.json")

    if "citas_medicas" not in df.columns or "pacientes" not in df.columns:
        raise RuntimeError("El JSON debe contener 'citas_medicas' y 'pacientes'.")

    df_pacientes = get_parsed_information(df, "pacientes")
    df_citas_medicas = get_parsed_information(df, "citas_medicas")


    #2. Transforms
    df_pacientes = apply_transformations_to_pacientes(df_pacientes)
    df_citas_medicas = apply_transformations_to_citas_medicas(df_citas_medicas)


    #3. Load

    #To test in local we will save the data in parquet (to optimize) files in data/processed folder and in .csv format to visualize the results
    df_pacientes.write.mode('overwrite').parquet('data/processed/parquet/pacientes')
    df_citas_medicas.write.mode('overwrite').parquet('data/processed/parquet/citas_medicas')

    df_pacientes.write.mode('overwrite').option("header", "true").csv('data/processed/csv/pacientes/pacientes_csv')
    df_citas_medicas.write.mode('overwrite').option("header", "true").csv('data/processed/csv/citas_medicas/citas_medicas_csv')


    # For production, we would write to a s3 data lake or a database
    # Example of writing to S3 in parquet format
    df_pacientes.repartition(1).write.mode("overwrite").format("parquet").save("s3a://mi-bucket/data/pacientes/")
    df_citas_medicas.repartition(1).write.mode("overwrite").format("parquet").save("s3a://mi-bucket/data/citas_medicas/")

    #Example of writing to a datawarehouse (Redhift)   
    df_pacientes.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://redshift-cluster-1.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev") \
        .option("dbtable", "public.pacientes") \
        .option("user", "myuser") \
        .option("password", "mypassword") \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .mode("overwrite") \
        .save()    


    df_citas_medicas.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://redshift-cluster-1.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev") \
        .option("dbtable", "public.citas_medicas") \
        .option("user", "myuser") \
        .option("password", "mypassword") \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .mode("overwrite") \
        .save()
    #========================================= END PIPELINE ==================================================




    '''
    df_pacientes.select(F.col('id_paciente'), F.col('fecha_nacimiento_iso'), F.col('fecha_nacimiento_std'), F.col('edad'), F.col('edad_calc'), F.col('edad_limpia')).show(20, truncate=False)
    df_pacientes.select('*').show()
    df_pacientes.filter(F.col('id_paciente') == 68).show()

    df_pacientes.select("ciudad").distinct().show()

    df_citas_medicas.select(F.count('*')).show()
    '''
