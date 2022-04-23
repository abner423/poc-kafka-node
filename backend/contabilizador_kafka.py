import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, desc, col, length


class SparkKafka:

    def __init__(self, argv):
        self.topicos = ','.join([argv[i] for i in range(3, len(argv))])
        self.broker = argv[2]
        self.timestamp = argv[1]

        self.spark = SparkSession.builder \
            .master('spark://192.168.0.21:7077') \
            .appName('SparkKafkaCluster') \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel('WARN')
        self.df = self.__inicializa_dataFrame()
        self.palavras = self.df.select(explode(split(self.df.value, ' ')) \
            .alias('value'), self.df.timestamp)

    def __inicializa_dataFrame(self):
        return self.spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.broker) \
            .option('subscribe', self.topicos) \
            .option('includeTimestamp', 'true') \
            .load()

    def _aplica_timestamp(self, query):
        return query \
            .withWatermark('timestamp', f'{self.timestamp} seconds') \
            .groupBy(
                window(col('timestamp'), f'{self.timestamp} seconds', f'{self.timestamp} seconds'),
                col('value')) \
            .count().sort(desc('window'))

    def retorna_stream_de_escrita(self, querys):
        return [self._aplica_timestamp(getattr(self, q)) \
            .writeStream \
            .outputMode('complete') \
            .format('console') \
            .option('truncate', 'false') \
            .start()
            for q in querys
        ]

    def espera_execucao(self, streams):
        for s in streams:
            s.awaitTermination()

    @property
    def filtra_por_char(self):
        return self.palavras \
            .select(col('timestamp'), col('value').substr(1, 1).alias('value')) \
            .where(col('value').rlike('^[SPR]')) \

    @property
    def filtra_por_quantidade(self):
        return self.palavras \
            .select(col('timestamp'), length(col('value')).alias('value')) \
            .where(col('value').isin([6, 8, 11])) \


if __name__ == '__main__':

    if len(sys.argv) < 4:
        print(
            'Uso correto: contabilizador_kafka.py <timestamp> <broker kafka> '
            '<lista de tópicos kafka>', file=sys.stderr
        )
        sys.exit(-1)

    sparkKafka = SparkKafka(sys.argv)
    streams = sparkKafka.retorna_stream_de_escrita([
        'palavras', 'filtra_por_char', 'filtra_por_quantidade'
    ])
    sparkKafka.espera_execucao(streams)
