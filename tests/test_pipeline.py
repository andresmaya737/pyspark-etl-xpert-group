
from pathlib import Path
import pytest
from pyspark.sql import functions as F, types as T, Row
from pyspark.sql.window import Window
from .helper_loader import load_module_from_path

ROOT = Path(__file__).resolve().parents[1]
pipeline_path = ROOT / "src/pipeline.py"
assert pipeline_path.exists(), "Ubica pipeline.py en el root del repo/proyecto."

# Carga el módulo pipeline.py dinámicamente
mod = load_module_from_path("pipeline", str(pipeline_path))

def test_parse_spanish_date():
    fn = mod.parse_spanish_date
    assert fn("1980-04-16") == "1980-04-16"
    assert fn("14 de diciembre de 2007") == "2007-12-14"
    assert fn("22 de oct de 2002") == "2002-10-22"
    assert fn("1959-06-33") is None


def test_clean_date_and_age(spark):
    df = spark.createDataFrame([
        Row(id=1, fecha_nacimiento="1980-04-16"),
        Row(id=2, fecha_nacimiento="14 de diciembre de 2007"),
        Row(id=3, fecha_nacimiento="1959-06-33"), 
    ])
    df = mod.clean_date(df, "fecha_nacimiento")
    assert df.filter(F.col("fecha_nacimiento").isNotNull()).count() == 2
    # calcula edad y verifica que no sea negativa
    df = mod.calculate_age(df, "edad", "fecha_nacimiento")
    assert df.filter(F.col("edad") < 0).count() == 0


def test_clean_gender(spark):
    df = spark.createDataFrame([
        Row(sexo="M"),
        Row(sexo="male"),
        Row(sexo="FEMALE"),
        Row(sexo="x"),
        Row(sexo=None)
    ])
    out = mod.clean_gender(df, "sexo").collect()
    vals = [r["sexo"] for r in out]
    assert "M" in vals and "F" in vals
    assert None in vals  # para 'x' y None


def test_clean_email(spark):
    df = spark.createDataFrame([
        Row(email="user@example.com"),
        Row(email="bad@"),
        Row(email=None)
    ])
    out = mod.clean_email(df, "email").collect()
    vals = [r["email"] for r in out]
    assert "user@example.com" in vals
    # inválidos deben quedar en None
    assert vals.count(None) >= 2


def test_clean_phone(spark):
    df = spark.createDataFrame([
        Row(telefono="+57 (300) 123-45-67"),
        Row(telefono="(604)123-4567"),
        Row(telefono="abc"),
        Row(telefono=None),
    ])
    out = mod.clean_phone(df, "telefono").collect()
    vals = [r["telefono"] for r in out]
    # Solo dígitos (se eliminan no-dígitos)
    assert vals[0].isdigit() and vals[1].isdigit()
    # 'abc' y None -> cadena vacía según la implementación (coalesce con "")
    assert "" in vals


def test_drop_duplicates(spark):
    df = spark.createDataFrame([
        Row(nombre="Ana", fecha="2000-01-01", ciudad="Bogota"),
        Row(nombre="Ana", fecha="2000-01-01", ciudad="Bogota"),
        Row(nombre="Ana", fecha="2000-01-02", ciudad="Bogota"),
    ])
    out = mod.drop_duplicates(df, subset_cols=["nombre","fecha","ciudad"])
    assert out.count() == 2  # se quitó 1 duplicado exacto

