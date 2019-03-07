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

const SnitchMessageState = {
    Received : "Received",
    InvalidMessage : "InvalidMessage",
    Completed : "Completed",
    Error : "Error"
}

const Snitch = (() => {

    let _config;
    let available = false;
    let client, producer;

    produceMessageToKafkaTopic = (topic, message) => {

        payloads = [{ topic , messages: message}];
        producer.send(payloads, function (err, data) {
            console.log(data);
        });
    }

    return {
        getConfig : () => {
            return _config
        },
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
           _config = config;

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
                        state : "Error",
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
        dispatch: (arguments) => {
            let { origin, dispatcher, context, event, message, executor, target, state, correlationId } = arguments
            try {
                produceMessageToKafkaTopic(snitchConfig.kafka.topics.default,{
                    metadata : {
                        origin,
                        dispatcher,
                        state,
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
        info: (arguments) => {
            let { origin, dispatcher, context, event, message, executor, target, state, correlationId } = arguments
            try {
                produceMessageToKafkaTopic(snitchConfig.kafka.topics.error,{
                    metadata : {
                        origin,
                        dispatcher,
                        state,
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
        heartbeat: (arguments) => {
            let { origin, dispatcher, context, event, message, executor, target, state, correlationId } = arguments
            try {
                produceMessageToKafkaTopic(snitchConfig.kafka.topics.error,{
                    metadata : {
                        origin,
                        dispatcher,
                        state,
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



module.exports = Snitch

/*
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
*/