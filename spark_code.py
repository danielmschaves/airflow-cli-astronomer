from pyspark.sql import SparkSession
import logging

# Configurando o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_csv_from_s3(spark, s3_path):
    """
    Função para ler um arquivo CSV do Amazon S3.

    Args:
        spark (SparkSession): SparkSession ativa.
        s3_path (str): Caminho do arquivo CSV no Amazon S3.

    Returns:
        DataFrame: DataFrame contendo os dados do arquivo CSV.
    """
    try:
        logger.info("Lendo arquivo CSV do Amazon S3...")
        df = spark.read.csv(s3_path, header=True, inferSchema=True, sep=";")
        logger.info("Arquivo CSV lido com sucesso.")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler arquivo CSV: {str(e)}")
        raise e

def write_parquet_to_s3(df, s3_path):
    """
    Função para escrever um DataFrame no formato Parquet no Amazon S3.

    Args:
        df (DataFrame): DataFrame a ser escrito no formato Parquet.
        s3_path (str): Caminho onde o arquivo Parquet será armazenado no Amazon S3.
    """
    try:
        logger.info("Escrevendo DataFrame no formato Parquet...")
        df.write.parquet(s3_path, mode="overwrite")
        logger.info("DataFrame escrito no formato Parquet com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao escrever DataFrame no formato Parquet: {str(e)}")
        raise e

if __name__ == "__main__":
    # Inicializando uma SparkSession
    spark = SparkSession.builder \
        .appName("Convert CSV to Parquet") \
        .getOrCreate()

    try:
        # Caminho do arquivo CSV no Amazon S3
        csv_s3_path = "s3://spark-airflow-197398273774/dados/raw/"

        # Leitura do arquivo CSV
        csv_df = read_csv_from_s3(spark, csv_s3_path)

        # Caminho onde o arquivo Parquet será armazenado no Amazon S3
        parquet_s3_path = "s3://spark-airflow-197398273774/dados/silver/"

        # Escrita do DataFrame no formato Parquet
        write_parquet_to_s3(csv_df, parquet_s3_path)

        logger.info("Processo concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no processo: {str(e)}")
    finally:
        # Encerrando a SparkSession
        spark.stop()
