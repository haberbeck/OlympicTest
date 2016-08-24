//Arquivo de configuração do express

//importando o express
var exp = require('express');
var load = require('express-load');
var body_parser = require('body-parser');


module.exports = function () {

var app = exp();

//configurando o express para utilizar o ejs para retornar o HTML
app.set('view engine','ejs');


//configuração para deixar uma pasta de CSS pública e estática
app.use(exp.static('./public'));


//configuração para a API receber entrada via HTML
app.use(body_parser.urlencoded({extended:true}));


//configuração para a API receber entrada via JSON
app.use(body_parser.json());


//configuração para o express utilizar o express-validator
app.use(require('express-validator')());



/*app.use(function(req, res, next){
	var ConnectionFactory = require('../persistencia/connectionFactory');
	var conn = new ConnectionFactory().getConnection();
	app.set('connection', conn);
	next();
	conn.end();
})*/


//configuração de rotas(controller)
require('../rotas/pesquisa')(app);


return app;


}