const avro = require('avsc');

module.exports = avro.Type.forSchema({
    type: 'record',
    fields: [
        {
            name: 'id',
            type: 'int'
        },
        {
            name: 'nome',
            type: 'string'
        },
        {
            name: 'email',
            type: 'string'
        },
        {
            name: 'idade',
            type: 'int',
        }
    ]
});