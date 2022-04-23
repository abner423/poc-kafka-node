import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, desc, col, length


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print(
            'Uso correto: contabilizador_kafka.py <broker kafka> '
            '<lista de tópicos kafka>', file=sys.stderr
        )
        sys.exit(-1)

    topicos = ','.join([sys.argv[i] for i in range(2, len(sys.argv))])

    spark = SparkSession.builder \
        .master('spark://192.168.0.21:7077') \
        .appName('SparkKafkaCluster') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', sys.argv[1]) \
        .option('subscribe', topicos) \
        .option('includeTimestamp', 'true') \
        .load()

    palavras = df.select(explode(split(df.value, ' ')).alias('palavra'), df.timestamp)

    palavras_total = palavras \
        .withWatermark('timestamp', '10 seconds') \
        .groupBy(
            window(palavras.timestamp, '10 seconds', '10 seconds'),
            palavras.palavra) \
        .count().sort(desc('window'))

    palavras_chars = palavras \
        .select(palavras.timestamp, palavras.palavra.substr(1, 1).alias('letra')) \
        .where(col('letra').rlike('^[SPR]')) \
        .withWatermark('timestamp', '10 seconds') \
        .groupBy(
            window(palavras.timestamp, '10 seconds', '10 seconds'),
            col('letra')) \
        .count().sort(desc('window'))

    palavras_quantidade = palavras \
        .select(palavras.timestamp, length(palavras.palavra).alias('quantidade')) \
        .where(col('quantidade').isin([6, 8, 11])) \
        .withWatermark('timestamp', '10 seconds') \
        .groupBy(
            window(palavras.timestamp, '10 seconds', '10 seconds'),
            col('quantidade')) \
        .count().sort(desc('window'))

    query_total = palavras_total \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', 'false') \
        .start()

    query_chars = palavras_chars \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', 'false') \
        .start()

    query_quatidade = palavras_quantidade \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', 'false') \
        .start()

    query_total.awaitTermination()
    query_chars.awaitTermination()
    query_quatidade.awaitTermination()
