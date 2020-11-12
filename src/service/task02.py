from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import array
from pyspark.sql.functions import count
from pyspark.sql.functions import months_between
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import current_date
from pyspark.sql.functions import date_format
from pyspark.sql.functions import concat
from pyspark.sql.functions import collect_list
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
        log("INFO", f"[readCSV] Lendo arquivo CSV: {file}.")
        dfEnd = self.spark.read.csv(file, header=True)
        return dfEnd

    def createAgeDateColumns(self, df):
        """Criar colunas de idade do cliente e data do pedido.
        
        Args:
            df: Dataframe
        
        Returns:
            Dataframe.
        """
        log("INFO", f"[createAgeDateColumns] Criando colunas de idade e data do pedido.")
        return (df.withColumn("data_pedido_date", 
                            date_format(from_unixtime(col('data_pedido')), "yyyy-MM-dd"))
                  .withColumn('idade',
                            (months_between(current_date(), col('data_nascimento_cliente'))/12).cast(IntegerType())))

    def filterShoppingBlackFriday(self, df):
        """Filtrar as compras realizadas nas últimas 3 Black Fridays.

        Args:
            df: Dataframe
        
        Returns:
            Dataframe.
        """
        log("INFO", f"[filterShoppingBlackFriday] Filtrando as compras das Black Fridays.")
        bf_date = ["2017-11-24",
                   "2017-11-25",
                   "2017-11-26",
                   "2018-11-23",
                   "2018-11-24",
                   "2018-11-25",
                   "2019-11-29",
                   "2019-11-30",
                   "2019-12-01"]
        return df.filter(col("data_pedido_date").isin(bf_date))

    def createOrderList(self, df):
        """Criando coluna com a lista dos pedidos.
           A coluna lista dos pedidos é formada por um array de arrays. 
           Com os valores de codigo do pedido e a data do pedido.

        Args:
            df: Dataframe
        
        Returns:
            Dataframe.
        """
        log("INFO", f"[createOrderList] Criando a lista dos pedidos.")
        dfStaging = df.withColumn("codigo_pedido_data", 
                                  array(concat(col('codigo_pedido'), 
                                               lit(', '),
                                               col('data_pedido_date'))))
        dfStaging = dfStaging.drop('data_nascimento_cliente', 
                                   'data_pedido', 
                                   'codigo_pedido',
                                   'idade')
        dfEnd = dfStaging.groupBy('codigo_cliente').agg(collect_list(col('codigo_pedido_data')).alias('lista_pedidos'))
        return dfEnd

    def filterCustomerOrders(self, df):
        """Filtrar clientes com mais de duas compras nos dias da Black Friday

        Args:
            df: Dataframe
        
        Returns:
            Dataframe.
        """
        log("INFO", f"[filterCustomerOrders] Filtrando clientes que fizeram duas compras nos dias da Black Friday.")
        dfStaging = (df.groupBy(col('codigo_cliente')) 
                       .agg(count('data_pedido').alias('numero_pedidos'))
                       .orderBy("numero_pedidos", ascending=False))
        dfEnd = dfStaging.filter(dfStaging.numero_pedidos > 2)
        return dfEnd

    def countOrders(self, df):
        """Calcular a quantidade de todos os pedidos por cliente.

        Args:
            df: Dataframe
        
        Returns:
            Dataframe.
        """
        log("INFO", f"[countOrders] Contando a quantidade de pedidos por cliente.")
        dfEnd = (df.groupBy(col('codigo_cliente')) 
                    .agg(count('data_pedido').alias('numero_pedidos'))
                    .orderBy("numero_pedidos", ascending=False))
        return dfEnd

    def filterAgeUnderThirty(self, df):
        """Filtrar os clientes que são menores de 30 anos e compraram na Black Friday.

        Args:
            df: Dataframe
        
        Returns:
            Dataframe.
        """
        log("INFO", f"[filterAgeUnderThirty] Filtrando os clientes com idade inferior a 30 anos.")
        dfStaging = df.dropDuplicates(['codigo_cliente'])
        dfStaging =  dfStaging.filter(df.idade < 30)
        dfEnd = dfStaging.select("codigo_cliente", "idade")
        return dfEnd

    def joinDataframes(self, dfOrders, dfCount, dfOrdersList, dfAge):
        """Faz o Join de dois DataFrames.

        Args:
            df: Dataframe.
        
        Returns:
            DataFrame: Dataframe completo.
        """
        log("INFO", "[joinDataFrame] Fazendo o Join dos DataFrames.")
        innerJoinAgeOrders = dfOrders.join(dfCount, 
                                        ["codigo_cliente"],
                                        "inner")
        innerJoinStaging = innerJoinAgeOrders.join(dfOrdersList, 
                                        ["codigo_cliente"],
                                        "inner")
        innerJoinEnd = innerJoinStaging.join(dfAge, 
                                            ["codigo_cliente"],
                                            "inner")
        return innerJoinEnd

    def writeCSV(self, df, file):
        """Escrever CSV.
        
        Args:
            df: Dataframe.
            file: Nome do arquivo CSV.
        """
        log("INFO", "[writeCSV] Salvando o CSV.")
        dfResultToCSV = df.withColumn("lista_pedidos", df['lista_pedidos'].cast("string"))
        dfResultToCSV.repartition(1).write.csv(file, mode='overwrite', header=True)
