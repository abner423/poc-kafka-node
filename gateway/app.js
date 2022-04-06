const express = require('express');
const app = express();
var bodyParser = require('body-parser');
const eventType = require('../eventType');
const Kafka = require('node-rdkafka');

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, {
    topic: 'email'
});

function writeMessage(user) {
    const success = stream.write(eventType.toBuffer(user));

    if (success) {
        console.log(`message queued (${JSON.stringify(user)})`);
    } else {
        console.log("Something wrong ..");
    }
}

let users = [{
    id: 1,
    nome: "Abner",
    email: "abner.f.c.r@hotmail.com",
    idade: 21
}];

app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())


app.get('/users', (req, res) => {
    res.status(200).send(users)
})

app.post('/users', (req, res) => {
    let { nome, email, idade } = req.body
    let id = 1
    if (users.length != 0)
        id = users[users.length - 1].id + 1

    let user = { id, nome, email, idade };

    users.push(user)
    writeMessage(user);

    res.status(200).send()
})

app.delete('/users/:id', (req, res) => {
    users = users.filter(user => user.id != req.params.id)

    res.status(200).send()
})

module.exports = app