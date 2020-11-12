from service.task01 import processingSpark as Task01
from service.task02 import processingSpark as Task02

def executeTask01():
    ss = Task01()  
    df = ss.readCSV("dataset/wordcount.txt")

    dfWords = ss.createDataFrameWords(df)
    dfLengthSmaller = ss.createDataFrameLength(dfWords, "smaller")
    dfLengthBigger = ss.createDataFrameLength(dfWords, "bigger")
    
    topWordsAndCountsDF = ss.wordCount(dfLengthSmaller).orderBy("count", ascending=False)
    newRow = ss.spark.createDataFrame([["MAIORES QUE 10", dfLengthBigger.count()]])
    dfResult = ss.joinDataFrames(topWordsAndCountsDF, newRow)

    ss.writeCSV(dfResult, 'result/task1')

def executeTask02():
    ss = Task02()
    df = ss.readCSV("dataset/clientes_pedidos.csv")

    dfComplete = ss.createAgeDateColumns(df)
    dfSalesJustBF = ss.filterShoppingBlackFriday(dfComplete)
    dfOrderList = ss.createOrderList(dfComplete)

    dfClientTwoOrders = ss.filterCustomerOrders(dfSalesJustBF)
    dfClientTwoOrders = dfClientTwoOrders.drop('numero_pedidos')
    
    dfClientCountOrders = ss.filterCustomerOrders(dfComplete)
    dfUnderThirty = ss.filterAgeUnderThirty(dfSalesJustBF)

    dfResult = ss.joinDataframes(dfClientTwoOrders, 
                              dfClientCountOrders, 
                              dfOrderList, 
                              dfUnderThirty)
    dfResult = dfResult.orderBy("numero_pedidos", ascending=False)
    ss.writeCSV(dfResult, 'result/task2')

if __name__ == "__main__":

    executeTask01()
    executeTask02()

