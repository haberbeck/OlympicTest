//Arquivo de inicialização do listener

var app = require("./config/custom-express")();



var server = app.listen("3000",function() {
        console.log("Subiu o servidor!");
});


