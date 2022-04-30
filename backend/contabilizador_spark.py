from pyspark import SparkContext

from pyspark.streaming import StreamingContext


if __name__ == '__main__':

    # Criação do contexto de ligação com o cluster de máquinas
    sc = SparkContext('spark://192.168.0.21:7077')
    # Settagem do nível de logger para warnings
    sc.setLogLevel('WARN')
    # Início do streaming com uma janela de 10 segundos
    ssc = StreamingContext(sc, 10)

    # Conexão de streaming via web sockets
    palavras = ssc.socketTextStream('192.168.0.21', 9999)\
            .flatMap(lambda linha: linha.split(' '))

    # Contagem do total de palavras
    palavras_total = palavras\
            .map(lambda palavra: (palavra, 1))\
            .reduceByKey(lambda a, b: a + b)

    # Contagem das palavras que iniciam com S, P ou R
    palavras_chars = palavras\
            .filter(lambda palavra: palavra.startswith(('S', 'P', 'R')))\
            .map(lambda palavra: (palavra[0], 1))\
            .reduceByKey(lambda a, b: a + b)

    # Contagem das palavras que tem o tamanho 6, 8 ou 11
    palavras_tam = palavras\
            .filter(lambda palavra: len(palavra) in (6, 8, 11))\
            .map(lambda palavra: (len(palavra), 1))\
            .reduceByKey(lambda a, b: a + b)

    # Printagem das querys
    palavras_total.pprint()
    palavras_chars.pprint()
    palavras_tam.pprint()

    # Abertura do streaming e ínicio do processamento
    ssc.start()
    ssc.awaitTermination()
