import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, window, desc, col,
    length, to_json, struct, from_json
)


class SparkKafka:

    def __init__(self, argv):
        self.topicos = ','.join([argv[i] for i in range(3, len(argv))])
        self.broker = argv[2]
        self.timestamp = argv[1]

        # Cria uma sessão que liga ao cluster spark
        self.spark = SparkSession.builder \
            .master('spark://192.168.0.21:7077') \
            .appName('SparkKafkaCluster') \
            .getOrCreate()

        # Setta o logger para warnings, inicializa o data frame de streaming
        # e já le as palavras que serão processadas (JSON)
        self.spark.sparkContext.setLogLevel('WARN')
        self.df = self.__inicializa_dataFrame()
        self.palavras = self.df.select(explode(split(
            from_json(self.df.value.cast('string'), 'resumo STRING').resumo,
            ' '
        )).alias('value'), self.df.timestamp)

    # Inicialização de data frame de streaming do kafka, são settadas
    # algumas configurações extras também
    def __inicializa_dataFrame(self):
        return self.spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.broker) \
            .option('subscribe', self.topicos) \
            .option('startingOffsets', 'latest') \
            .option('failOnDataLoss', 'false') \
            .option('includeTimestamp', 'true') \
            .load() \

    # Método que aplica a timestamp com watermark para respostas atrasadas.
    # Além disso, também é definido a janela, realizado o agrupamento por
    # palavras, sua contagem e ordenação por janela.
    def _aplica_timestamp(self, query):
        return query \
            .withWatermark('timestamp', f'{self.timestamp} seconds') \
            .groupBy(
                window(col('timestamp'), f'{self.timestamp} seconds', f'{self.timestamp} seconds'),
                col('value')) \
            .count().sort(desc('window'))

    # Método responsável por receber um array de querys e adicionar os
    # comandos de streaming de saida, bem como formatação JSON
    def retorna_stream_de_escrita(self, querys):
        return [self._aplica_timestamp(getattr(self, q)) \
            .select(to_json(struct('*')).alias('json')) \
            .selectExpr('json AS value') \
            .writeStream \
            # .option("checkpointLocation", "hdfs://192.168.0.21:9000/spark/") \
            # .format('kafka') \
            # .option('kafka.bootstrap.servers', self.broker) \
            # .option('topic', 'testcluster2') \
            .outputMode('complete') \
            .format('console') \
            .option('truncate', 'false') \
            .start()
            for q in querys
        ]

    # As querys vão ser processadas apenas nessa função com o awaitTermination
    def espera_execucao(self, streams):
        for s in streams:
            s.awaitTermination()

    # A partir desse ponto, são querys específicas que possuem suas ordenações,
    # filtragens, entre outros comandos de bancos adicionados aqui
    @property
    def filtra_por_char(self):
        return self.palavras \
            .select(col('timestamp'), col('value').substr(1, 1).alias('value')) \
            .where(col('value').rlike('^[SPR]')) \

    @property
    def filtra_por_quantidade(self):
        return self.palavras \
            .select(col('timestamp'), length(col('value')).alias('value').cast('string')) \
            .where(col('value').isin([6, 8, 11])) \


if __name__ == '__main__':

    if len(sys.argv) < 4:
        print(
            'Uso correto: contabilizador_kafka.py <timestamp> <broker kafka> '
            '<lista de tópicos kafka>', file=sys.stderr
        )
        sys.exit(-1)

    # Fluxo principal de inicialização da conexão/streaming, criação das 
    # querys com os streamings de saidas configurados e execução das
    # mesmas de forma distribuida
    sparkKafka = SparkKafka(sys.argv)
    streams = sparkKafka.retorna_stream_de_escrita([
        'palavras', 'filtra_por_char', 'filtra_por_quantidade'
    ])
    sparkKafka.espera_execucao(streams)
