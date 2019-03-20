var snitchConfig = require('./config');

/*
var consul = require('consul')({
    host: config.consul.address,
    port: config.consul.port
});
*/


var kafka = require('kafka-node')
    Producer = kafka.Producer
    

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
        
        if(typeof message === "object"){
            try{
                message = JSON.stringify(message)
            }
            catch(e){
                console.error(e);
            }
        }
        payloads = [{ topic , messages: [message]}];
        producer.send(payloads, function (err, data) {
            //console.log(data);
        });
    }

    return {
        MessageStateTypes : SnitchMessageState,
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
        __dispatchMessageWithoutMetadata__: () => {
            produceMessageToKafkaTopic(snitchConfig.kafka.topics.default, 
                {
                  "message": {
                    "step": {
                      "_id": "1",
                      "Name": "Approve Activity",
                      "BuildingBlockArtifactId": "5b8d2180bc655759bd75f5d1"
                    }
                }
            })
        },
        __dispatchMessageWithMetadataAndUnknownType__: () => {
            produceMessageToKafkaTopic(snitchConfig.kafka.topics.default, 
                {
                    "metadata": {
                    "origin": "ActionItems",
                    "dispatcher": "ActionItems",
                    "state": "completed",
                    "context": "Snitch node Test",
                    "event": "Test",
                    "type": "SomethingDifferent",
                    "executor": "ActionItems",
                    "target": "ActionItems",
                    "correlationId": "de05edef-a766-48d5-a7ac-b71705722789"
                  },
                  "message": {
                    "step": {
                      "_id": "1",
                      "Name": "Approve Activity",
                      "BuildingBlockArtifactId": "5b8d2180bc655759bd75f5d1"
                    }
                }
            })
        },
        getUUID : () => {
            return uuidv4();
        },
        error: (arguments) => {
            let { origin, dispatcher, context, event, message, executor, target, correlationId } = arguments

            if(typeof message==="string"){
                return 
            }
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
                    ...message})    
            } catch (err) {
                console.log(err.stack)
            }
        },
        dispatch: (arguments) => {
            let { origin, dispatcher, context, event, type, message, executor, target, state, correlationId } = arguments
            if(typeof message==="string"){
                return 
            }
            try {
                produceMessageToKafkaTopic(snitchConfig.kafka.topics.default,{
                    metadata : {
                        origin,
                        dispatcher,
                        state,
                        context,
                        event,
                        type,
                        executor,
                        target,
                        correlationId : correlationId || uuidv4(),
                    },
                    ...message})    
            } catch (err) {
                console.log(err.stack)
            }
        },
        info: (arguments) => {
            let { origin, dispatcher, context, event, message, executor, target, state, correlationId } = arguments

            if(typeof message==="string"){
                return 
            }

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
                    ...message})    
            } catch (err) {
                console.log(err.stack)
            }
        },
        heartbeat: (arguments) => {
            let { origin, dispatcher, context, event, message, executor, target, state, correlationId } = arguments
            if(typeof message==="string"){
                return 
            }
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
                    ...message})    
            } catch (err) {
                console.log(err.stack)
            }
        }
    }
})()


const uuidv4 = function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}


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

    console.log(Snitch.getUUID());

    setTimeout(() => {
        demoDispatcher()    
    }, 1000)


    demoDispatcher = () => {
      
        Snitch.dispatch({message : { "username" : "Relix" }})

        Snitch.dispatch({
            origin : 'Demo', 
            dispatcher : 'Snitch Demo', 
            context : 'Snitch node Test', 
            event : 'Test',
            type : 'Demo',
            executor : "self timeout",
            state : SnitchMessageState.Completed,
            message : { "username" : "Relix" }
        })
      
        setTimeout(() => {
            demoDispatcher()    
        }, 10000000)
    }

    */