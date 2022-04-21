from pyspark import SparkContext

from pyspark.streaming import StreamingContext


if __name__ == '__main__':

    sc = SparkContext('spark://192.168.0.21:7077')
    sc.setLogLevel('WARN')
    ssc = StreamingContext(sc, 10)

    palavras = ssc.socketTextStream('192.168.0.21', 9999)\
            .flatMap(lambda linha: linha.split(' '))

    palavras_total = palavras\
            .map(lambda palavra: (palavra, 1))\
            .reduceByKey(lambda a, b: a + b)

    palavras_chars = palavras\
            .filter(lambda palavra: palavra.startswith(('S', 'P', 'R')))\
            .map(lambda palavra: (palavra, 1))\
            .reduceByKey(lambda a, b: a + b)

    palavras_tam = palavras\
            .filter(lambda palavra: len(palavra) in (6, 8, 11))\
            .map(lambda palavra: (palavra, 1))\
            .reduceByKey(lambda a, b: a + b)

    palavras_total.pprint()
    palavras_chars.pprint()
    palavras_tam.pprint()

    ssc.start()
    ssc.awaitTermination()
