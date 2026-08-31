
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf
import os

Config = SparkConf()
Config.set("spark.sql.repl.eagerEval.enabled", True)
Config.set("spark.sql.repl.eagerEval.maxNumRows", "20")
Config.set("spark.sql.repl.eagerEval.truncate", "-1")

spark = SparkSession.builder.config(conf=Config).master("local[*]").appName("ColabSpark").getOrCreate()

_filename = 'PR_202401_HOSP_CONS'
_file_path = f'/content/drive/My Drive/Colab Notebooks/pipeline/{_filename}.csv'

if not os.path.exists(_file_path):
    print(f"Error: File not found at {_file_path}. Please check the path and ensure Google Drive is mounted.")
else:
    os.makedirs('/content/Data', exist_ok=True)
    _df = pd.read_csv(_file_path, sep=';', encoding='UTF-8',dtype=str, keep_default_na=False)
    _df.to_parquet(f'/content/Data/{_filename}.parquet')

df_raw = spark.read.parquet(f'/content/Data/{_filename}.parquet')

df_raw = df_raw.select(*[nullif(df_raw[column_name], lit('')).alias(column_name) for column_name in df_raw.columns])

df_raw = df_raw.withColumn(
            "NM_MODALIDADE",
            when(
                col("NM_MODALIDADE") == "Seguradora Especializada Em Saúde",
                "seguradora_especializada_em_saude"
            )
            .when(
                col("NM_MODALIDADE") == "Cooperativa Médica",
                "cooperativa_medica"
            )
            .when(
                col("NM_MODALIDADE") == "Medicina De Grupo",
                "medicina_de_grupo"
            )
            .when(
                col("NM_MODALIDADE") == "Autogestão",
                "autogestao"
            )
            .when(
                col("NM_MODALIDADE") == "Filantropia",
                "filantropia"
            )
            .otherwise("nao_classificado"))

df_raw = df_raw.select(
    to_date(nullif(df_raw["ANO_MES_EVENTO"],lit('0')), "yyyy-MM").alias("ANO_MES_EVENTO"),
    df_raw["ID_EVENTO_ATENCAO_SAUDE"].try_cast(LongType()).alias("ID_EVENTO_ATENCAO_SAUDE"),
    df_raw["ID_PLANO"].try_cast(IntegerType()).alias("ID_PLANO"),
    df_raw["TEMPO_DE_PERMANENCIA"].try_cast(IntegerType()).alias("TEMPO_DE_PERMANENCIA"),
    df_raw["QT_DIARIA_ACOMPANHANTE"].try_cast(IntegerType()).alias("QT_DIARIA_ACOMPANHANTE"),
    df_raw["QT_DIARIA_UTI"].try_cast(IntegerType()).alias("QT_DIARIA_UTI"),
    df_raw["LG_VALOR_PREESTABELECIDO"].try_cast(IntegerType()).alias("LG_VALOR_PREESTABELECIDO"),
    df_raw["IND_ACIDENTE_DOENCA"].try_cast(IntegerType()).alias("IND_ACIDENTE_DOENCA"),
    df_raw["FAIXA_ETARIA"].try_cast(StringType()).alias("FAIXA_ETARIA"),
    df_raw["SEXO"].try_cast(StringType()).alias("SEXO"),
    df_raw["PORTE"].try_cast(StringType()).alias("PORTE"),
    df_raw["NM_MODALIDADE"].try_cast(StringType()).alias("NM_MODALIDADE"),
    df_raw["UF_PRESTADOR"].try_cast(StringType()).alias("UF_PRESTADOR"),
    df_raw["CD_MUNICIPIO_BENEFICIARIO"].try_cast(StringType()).alias("CD_MUNICIPIO_BENEFICIARIO"),
    df_raw["CD_MODALIDADE"].try_cast(StringType()).alias("CD_MODALIDADE"),
    df_raw["CD_MUNICIPIO_PRESTADOR"].try_cast(StringType()).alias("CD_MUNICIPIO_PRESTADOR"),
    df_raw["CD_CARATER_ATENDIMENTO"].try_cast(StringType()).alias("CD_CARATER_ATENDIMENTO"),
    df_raw["CD_TIPO_INTERNACAO"].try_cast(StringType()).alias("CD_TIPO_INTERNACAO"),
    df_raw["CD_REGIME_INTERNACAO"].try_cast(StringType()).alias("CD_REGIME_INTERNACAO"),
    df_raw["CD_MOTIVO_SAIDA"].try_cast(StringType()).alias("CD_MOTIVO_SAIDA"),

    df_raw["CID_1"].try_cast(StringType()).alias("CID_1"),
    df_raw["CID_2"].try_cast(StringType()).alias("CID_2"),
    df_raw["CID_3"].try_cast(StringType()).alias("CID_3"),
    df_raw["CID_4"].try_cast(StringType()).alias("CID_4"),

)

df_raw = df_raw.na.fill({'SEXO': "NAO_INFORMADO",
                         'CD_MUNICIPIO_BENEFICIARIO': "-1",
                         'CD_MUNICIPIO_PRESTADOR': "-1",
                         'CD_CARATER_ATENDIMENTO': "-1",
                         'CD_TIPO_INTERNACAO': "-1",
                         'CD_REGIME_INTERNACAO': "-1",
                         'CD_MOTIVO_SAIDA': "-1",
                         'IND_ACIDENTE_DOENCA': -1 })

df_raw.printSchema()

df_raw.repartition(1).write.partitionBy("NM_MODALIDADE").parquet(f"Data/trusted_data", mode='overwrite')
