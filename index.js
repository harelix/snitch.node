
var consul = require('consul')({
    host: "10.1.40.204",
    port: 8500
});

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

    produceMessageToKafkaTopic = (message) => {

        payloads = [{ topic: config.snitchkafkaTopic, messages: message}];
        producer.on('ready', function () {
            available = true
            producer.send(payloads, function (err, data) {
                console.log(data);
            });
        });
        producer.on('error', function (err) {})
    }

    return {
        init: () => {
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
        },
        error: () => {
            let { source, application, action, message, correlation, event } = arguments
            try {
                produceMessageToKafkaTopic({
                    metadata : {
                        correlationID : correlation || uuidv4(),
	                    type : 'SNITCH'
                    }, 
                    source, 
                    application, 
                    action, 
                    type : 'error',
                    message, 
                    event })    
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
    kafkaTopic: 'snitch', //default
    consulAddress : '10.1.40.204' //must set this value
})

setTimeout(() => {
    Snitch.error({
        source : 'Valerian', 
        application : 'Test Snitch', 
        action : 'Action', 
        message : 'Message', 
        correlation : 'correlation', 
        event : 'event'
    })
}, 5000)



  
  