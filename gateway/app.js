const express = require('express');
const cors = require('cors');
const app = express();
app.use(cors());
var bodyParser = require('body-parser');
const eventType = require('../eventType');
const Kafka = require('node-rdkafka');

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, {
    topic: 'user'
});

function writeMessage(user) {
    const success = stream.write(JSON.stringify(user));

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
    resumo: "Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of de Finibus Bonorum et Malorum (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, Lorem ipsum dolor sit amet.., comes from a line in section 1.10.32. The standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from de Finibus Bonorum et Malorum by Cicero are also reproduced in their exact original form, accompanied by English versions from the 1914 translation by H. Rackham."
}];

app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())


app.get('/users', (req, res) => {
    res.status(200).send(users)
})

app.post('/users', (req, res) => {
    let { nome, email, resumo } = req.body
    let id = 1
    if (users.length != 0)
        id = users[users.length - 1].id + 1

    let user = { id, nome, email, resumo };

    users.push(user)
    writeMessage(user);

    res.status(200).send()
})

app.delete('/users/:id', (req, res) => {
    users = users.filter(user => user.id != req.params.id)

    res.status(200).send()
})

module.exports = app