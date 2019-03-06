var snitchConfig = require('./config');

/*
var consul = require('consul')({
    host: config.consul.address,
    port: config.consul.port
});
*/


var kafka = require('kafka-node')
    Producer = kafka.Producer
    

function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}


const Snitch = (() => {

    let config;
    let available = false;
    let client, producer;

    produceMessageToKafkaTopic = (topic, message) => {

        payloads = [{ topic , messages: message}];
        producer.send(payloads, function (err, data) {
            console.log(data);
        });
    }

    return {
        init: (args) => {
            let { useHTTP, config } = args
            /*
            consul.kv.get('snitch/development/clients/core/config', function (err, result) {
                if (err) throw err;
                try{
                    config = JSON.parse(result.Value)
                    client = new kafka.KafkaClient({
                        kafkaHost : config.kafkaBrokerAddress + ":" + config.kafkaBrokerPort
                    }),
                    producer = new Producer(client);
                }
                catch(exception){
                    throw exception
                }
            });
            */

           client = new kafka.KafkaClient({
                kafkaHost : config.kafka.address
            }),
            producer = new Producer(client);
            producer.on('ready', () => {
                available = true
            });

            producer.on('error', (err) => {
                console.error(err)
            });
        },
        error: (arguments) => {
            let { origin, dispatcher, context, event, message, executor, target, correlationId } = arguments
            try {
                produceMessageToKafkaTopic(snitchConfig.kafka.topics.error,{
                    metadata : {
                        origin,
                        dispatcher,
                        context,
                        event,
                        type : snitchConfig.messages.SnitchMessage,
                        executor,
                        target,
                        correlationId : correlationId || uuidv4(),
                    },
                    message})    
            } catch (err) {
                console.log(err.stack)
            }
        },
        info: () => {
            info(arguments)
        },
        heartbeat: () => {
            info(arguments)
        }
    }
})()

const info = async (args) => {

  
    let { server, application, action, message, correlation, event } = args
    try {
        
    } catch (err) {
        console.log(err.stack)
    }
}


const heartbeat = async (args) => {

   
    let { server, application, action, message, correlation, event } = args
    try {
        
    } catch (err) {
        console.log(err.stack)
    }
}



module.exports = {
    Snitch
}

/**
 * 
 * 
 *  Example on how to use Snitch
    Snitch.init({
        useHTTP : false, //defaults to kafka 
        kafkaTopic: 'snitch', //default
        consulAddress : '10.1.40.204' //must set this value
    })
**/

Snitch.init({
    useHTTP : false, //defaults to kafka 
    config : snitchConfig //consulAddress : when valerian will go online in 2025, 
})

setTimeout(() => {
    demoDispatcher()    
}, 5000)


demoDispatcher = () => {
    Snitch.error({
        origin : 'Valerian', 
        context : 'Snitch node Test', 
        dispatcher : 'Self-Snitch', 
        event : 'Test',
        executor : "self timeout",
        message : "error test message from snitch"
    })
    setTimeout(() => {
        demoDispatcher()    
    }, 1000)
}