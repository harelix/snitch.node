

module.exports = {
    consul : {
        "address" : "10.1.40.190:8500",
        "port" : 8500
    },
    kafka : {
        address : "vienna-bi-srv01.managix.local:9092",
        topics : {
            error : "snitch_error",
            default : "xvulcanx"
        }
    },
    messages : {
        RogueMessage : "RogueMessage",
        SnitchMessage : "SnitchMessage"
    }
}