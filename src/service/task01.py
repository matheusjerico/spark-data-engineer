from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql.functions import length
from pyspark.sql.functions import trim
from pyspark.sql.functions import lower
from utils.logging_utils import log

class processingSpark():
    def __init__(self):
        """Criar Spark Session
        """
        self.spark = SparkSession.builder \
                        .master("local") \
                        .appName("Desafio LuizaLabs") \
                        .getOrCreate()

    def readCSV(self, file):
        """Ler CSV.
        
        Args:
            file: Nome do arquivo CSV.
        
        Returns:
            DataFrame: Conteúdo do CSV.
        """
        log("INFO", f"[readCSV] Lendo arquivo CSV: {file}")
        dfEnd = self.spark.read.text(file).select(self.removePunctuation(col('value')))
        return dfEnd


    def removePunctuation(self, column):
        """Remover pontuação do dataframe
        
        Args:
            column: Coluna para remover as pontuações.
        
        Returns:
            Registros sem as pontuações.
        """
        log("INFO", f"[removePunctuation] Removendo pontuação da coluna: {column}")
        return trim(lower(regexp_replace(column, '[^\sa-zA-Z0-9]', ''))).alias('value')

    def createDataFrameWords(self, df):
        """Criar dataframe com uma palavra por linha.

        Args:
            df: Dataframe.
        
        Returns:
            DataFrame: Dataframe contendo as palavras.
        """
        log("INFO", "[createDataFrameWords] Criando DataFrame com uma palavra por linha.")
        dfWords = (df
                .select(explode(split(df.value, ' ')).alias('word'))
                .where(col('word') != ''))
        return dfWords

    def wordCount(self, wordListDF):
        """Cria dataframe com a contagem de palavras

        Args:
            wordListDF: Dataframe com uma coluna chamada 'word'.

        Returns:
            DataFrame: Dataframe contendo 'word' e 'count'.
        """
        log("INFO", "[wordCount] Contando as palavras.")
        return wordListDF.groupBy('word').count()

    def createDataFrameLength(self, df, flag):
        """Cria um DataFrame com o tamanho das palavras e aplica um filtro.

        Args:
            df: Dataframe.
        
        Returns:
            DataFrame.
        """
        log("INFO", "[createDataFrameLength] Criando uma coluna com o tamanho de cada palavra.")
        dfStaging = df.withColumn("length", length("word"))
        if flag == "smaller":
            dfEnd = dfStaging.filter(length(col('word')) <= 10)
        elif flag == "bigger":
            dfEnd = dfStaging.filter(length(col('word')) > 10)
        else:
            dfEnd = None
        return dfEnd

    def joinDataFrames(self, df, newRow):
        """Faz o Join de dois DataFrames.

        Args:
            df: Dataframe.
        
        Returns:
            DataFrame: Dataframe completo.
        """
        log("INFO", "[joinDataFrame] Fazendo o Join dos DataFrames.")
        dfEnd = df.union(newRow)
        return dfEnd

    def writeCSV(self, df, file):
        """Escrever CSV.
        
        Args:
            df: Dataframe.
            file: Nome do arquivo CSV.
        """
        log("INFO", "[writeCSV] Salvando o CSV.")
        df.repartition(1).write.csv(file, mode='overwrite')