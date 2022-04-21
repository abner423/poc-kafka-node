from pyspark import SparkContext

from pyspark.streaming import StreamingContext


if __name__ == '__main__':

    sc = SparkContext('spark://192.168.0.21:7077')
    sc.setLogLevel('WARN')
    ssc = StreamingContext(sc, 10)

    palavras = ssc.socketTextStream('192.168.0.21', 9999)\
                  .flatMap(lambda linha: linha.split(' '))

    palavras_map = palavras.map(lambda palavra: (palavra, 1))
    total_por_palavra = palavras_map.reduceByKey(lambda a, b: a + b)

    total_por_palavra.pprint()

    ssc.start()
    ssc.awaitTermination()
