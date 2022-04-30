const Kafka = require('node-rdkafka');
const eventType = require('../eventType');
var nodemailer = require('nodemailer');
const express = require('express');
const cors = require('cors');
const app = express();
var bodyParser = require('body-parser');
app.use(cors());
app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())

const fs = require('fs')

var transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: 'pspdunb@gmail.com',
        pass: 'pspd123@'
    }
});

var consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092',
}, {});

consumer.connect();

consumer.on('ready', () => {
    console.log('consumer ready for receive user data..')
    consumer.subscribe(['user']);
    consumer.consume();
}).on('data', function (data) {
    // let user = eventType.fromBuffer(data.value)
    let user = JSON.parse(data.value)
    console.log(`Mensagem recebida, ${JSON.stringify(user)} enviando email ...`);
    const mailOptions = {
        from: 'pspdunb@gmail.com', // sender address
        to: user.email, // list of receivers
        subject: 'Relatorio', // Subject line
        text: `Ola ${user.nome}, você acaba de ser cadastrado na nossa api de usuários, se não for você por favor nos reporte via email\n`
    }
    transporter.sendMail(mailOptions, function (error, info) {
        if (error) {
            console.log(error);
        } else {
            console.log('Email enviado: ' + info.response);
        }
    });
}).on('event.error', function (error) {
    console.log("Erro aqui ", error)
});

app.post('/sendMail', (req, res) => {
    res.status(200).send("ola")
})

// consumer.on('ready', () => {
//     console.log('consumer ready for receive report data..')
//     consumer.subscribe(['report']);
//     consumer.consume();
// }).on('data', function (data) {
//     let user = eventType.fromBuffer(data.value)
//     const mailOptions = {
//         from: 'pspdunb@gmail.com', // sender address
//         to: 'abner.f.c.r@gmail.com', // list of receivers
//         subject: 'Relatorio', // Subject line
//         attachments: [{   // stream as an attachment
//             filename: 'image.jpg',
//             content: fs.createReadStream('./relatorio/relatorio.jpg')
//         }]
//     };
//     console.log(`sending mail about report...\n`);
//     console.log(`Ola ${user.nome},`)
//     console.log(`Você acaba de ser cadastrado na nossa api de usuários com email ${user.email}, se não for você por favor nos reporte via email\n`);
// });
module.exports = app