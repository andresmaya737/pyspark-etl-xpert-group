# 🏥 Pipeline using Pyspark for medical information

> Limpieza, validación y deduplicación de **`pacientes`** y **`citas_medicas`**.  
> Soporta fechas en español, normaliza género/email/teléfono, valida FK, deduplica por claves naturales y exporta a **S3** (vía `s3a://` o `boto3`). Incluye **tests con PyTest**.

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Tests](https://img.shields.io/badge/tests-pytest-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)
---

## Tabla de contenido
- [Descripción](#descripción)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Ejecución del pipeline](#ejecución-del-pipeline)
- [Exportar a S3](#exportar-a-s3)
  - [Con `s3a://` (Spark)](#con-s3a-spark)
  - [Con `boto3` (SDK)](#con-boto3-sdk)
- [Pruebas (PyTest)](#pruebas-pytest)
- [Reglas de calidad de datos](#reglas-de-calidad-de-datos)
- [Troubleshooting](#troubleshooting)
- [Entregables sugeridos](#entregables-sugeridos)
- [Licencia](#licencia)

---

## Descripción
Este repositorio contiene un **pipeline de calidad de datos con PySpark** para un dataset hospitalario con dos entidades:

- **`pacientes`**
- **`citas_medicas`**

El pipeline:
- **Normaliza fechas** (ISO y español: `14 de diciembre de 2007`, `22 de oct de 2002`).
- **Recalcula edad** a partir de la fecha de nacimiento y concilia con la provista.
- **Normaliza** género (`M`/`F`), **valida email**, y **sanitiza teléfonos** (solo dígitos).
- **Deduplica** por claves naturales.
- **Valida FK**: `citas_medicas.id_paciente` existe en `pacientes`.
- **Marca inconsistencias** (p. ej., citas “**completadas**” con fecha futura).
- Escribe salidas limpias en **Parquet/CSV** a disco o **S3**.

---

## Estructura del proyecto
```
.
├── src/pipeline.py                         # Lógica principal (PySpark)
├── data/
│   │   └── dataset_hospital 2.json       # Input
│   └── processed/
│       ├── parquet/
│       │   ├── pacientes/              # Salidas limpias Parquet
│       │   └── citas_medicas/
│       └── csv/
│       │   ├── pacientes/              # Salidas limpias csv
│       │   └── citas_medicas/                    
├── tests/
│   ├── conftest.py                     # Fixture Spark
│   ├── helper_loader.py                # Carga módulo por ruta (sin PYTHONPATH)
│   └── test_pipeline.py                # Tests unitarios de pipeline.py
├── requirements.txt                    # pyspark, pytest, boto3, etc.
└── README.md
```


---

## Requisitos
- **Python** 3.9+
- **PySpark** 3.x
- **PyTest** (tests)
- **boto3** (si subirás a S3 por SDK)

---

## Instalación
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# o:
pip install pyspark pytest boto3
```

---

## Ejecución del pipeline

### 1) Datos de entrada
Coloca el JSON en:
```
data/dataset_hospital 2.json
```

### 2) Ejecutar (local)


### 3) Salidas esperadas
- Parquet:
  ```
  data/processed/parquet/pacientes/
  data/processed/parquet/citas_medicas/
  data/processed/csv/pacientes/
  data/processed/csv/citas_medicas/
  ```

---

## Exportar a S3

### Con `s3a://` (Spark)
**Recomendado** para datos grandes/particionados.

## Pruebas (PyTest)

Estructura:
```
tests/
  conftest.py          # SparkSession local[2]
  helper_loader.py     # Importa pipeline.py por ruta
  test_pipeline.py     # Tests unitarios
```

Ejecuta:
```bash
pytest -q 
```

## Reglas de calidad de datos

**Fechas**
- `pacientes.fecha_nacimiento` → `date`.
- `citas_medicas.fecha_cita` → `date`.
- Soporte para meses en español (`enero..diciembre` y `ene..dic`). <- Esta funcion fue dada por IA
- Fechas imposibles → `null` (o se marcan para auditoría).

**Edad**
- Recalcular desde `fecha_nacimiento`.
- Concilia: si `|edad_prov – edad_calc| ≤ 2`, conserva; si no, usa `edad_calc`.

**Género**
- Normaliza a `M`/`F` (`male/M → M`, `female/F → F`); otros → `null`.

**Email**
- Regex simple RFC-like; inválidos → `null`.

**Teléfono**
- Remueve **todo lo no numérico** (quedan solo dígitos).

**Duplicados**
- `pacientes`: clave natural `(nombre, fecha_nacimiento, ciudad)`.
- `citas_medicas`: `(id_paciente, fecha_cita, especialidad)` con prioridad  
  `completada` > `cancelada` > `reprogramada`, luego costo no nulo, luego menor `id_cita`.

**Cruces**
- `citas_medicas.id_paciente` **debe existir** en `pacientes`.


