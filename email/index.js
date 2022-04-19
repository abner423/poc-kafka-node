const Kafka = require('node-rdkafka');
const eventType = require('../eventType');

var consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092',
}, {});

consumer.connect();

consumer.on('ready', () => {
    console.log('consumer ready..')
    consumer.subscribe(['user']);
    consumer.consume();
}).on('data', function (data) {
    let user = eventType.fromBuffer(data.value)
    console.log(`sending mail ...\n`);
    console.log(`Ola ${user.nome},`)
    console.log(`Você acaba de ser cadastrado na nossa api de usuários com email ${user.email}, se não for você por favor nos reporte via email\n`);
});