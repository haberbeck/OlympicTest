//Arquivo de configuração da rota /pesquisa


//importanto o twitter
var Twitter = require('twitter');

var cassandra = require('cassandra-driver');
var Uuid = cassandra.types.Uuid;

//instancia arquivo de config
//var config = require('../config');
var config = require("../config.json")



//instanciando o express
module.exports = function(app) {


    //DEFINIÇÃO DE ROTAS DISPONIVEIS


    app.get('/pesquisa',function(req,res){

        //o render retorna uma hastag vazia no primeiro carregamento
        //  porque não tem dados para carregar
        res.render('form', {hastag:{}});
    })


    //redireciona o request da raiz para a home page
    app.get('/',function(req,res){
        res.redirect('home');
    });


    //redireciona o request da raiz para a home page
    app.get('/home',function(req,res){
        res.render('home');
    });



    //realiza busca no twitter por palavra digitada
    app.post('/pesquisa', function(req, res) {

        req.assert('hastag','Atenção: hashtag é obrigatório.').notEmpty();
        //validação de erros
        var erros = req.validationErrors();
        if(erros){
            //retorna a msg de erro e também o objeto que foi enviado, para preencher novamente o formulário
            res.render('form', {err:erros, hastag:''});

            //retorno erro 400 para o usuário
            res.status(400);
            return;
        }

        var _hashtag = req.body.hastag;
        var registro = [];
        //parametros para fazer a busca no twitter
        var parametros ={
            q: _hashtag, 
            count: config.twitter.COUNT,
            result_type: config.twitter.RESULT_TYPE
        };

        //cria objeto do twitter
        var client_twitter = new Twitter({
          consumer_key: config.twitter.CONSUMER_KEY,
          consumer_secret: config.twitter.CONSUMER_SECRET,
          access_token_key: config.twitter.ACCESS_TOKEN_KEY,
          access_token_secret: config.twitter.ACCESS_TOKEN_SECRET
      });

        //realiza a pesquisa na API do twitter
        client_twitter.get('search/tweets', parametros, function(error, tweets, response) {
            if(!error){

                var hashtag, user_id, name, screen_name, followers_count, lang, tweet, date, schedule, hour;

                for(var i=0; i<tweets.statuses.length; i++){
                    var item = tweets.statuses[i];

                    //monta objeto com informações necessarias
                    hashtag = _hashtag;
                    user_id = item.user.user_id;
                    name = item.user.name;
                    screen_name = item.user.screen_name;
                    followers_count = item.user.followers_count;
                    lang = item.lang;
                    tweet = item.text;

                    //tratamento de datas
                    var data_tweet = new Date(item.created_at)
                    date = ('0' + data_tweet.getDate()).slice(-2) + '/' + ('0' +(data_tweet.getMonth() + 1)).slice(-2)  + '/' + data_tweet.getFullYear();
                    schedule =  data_tweet.getHours() + ':' + (data_tweet.getMinutes() + 1)  + ':' + data_tweet.getSeconds();
                    hour = data_tweet.getHours();
                    
                    //insere objeto no array
                    registro.push(
                    {
                        hashtag: _hashtag,
                        user_id: item.user.user_id,
                        name: item.user.name,
                        screen_name: item.user.screen_name,
                        followers_count: item.user.followers_count,
                        lang: item.lang,
                        tweet: item.text
                    });
                }
            }
            else
            {
                //se deu erro na pesquisa do twitter
                res.status(500);
                res.render('erros/500',{erroServer:error});
                return;
            }

            //retorna em html ou JSON
            res.format({
                json:function(){
                    //retorno JSON
                    res.json(resultado);
                },
                //retorno HTML
                html:function(){
                    res.render('lista', {pesquisa:registro});
                }
            })
        });
    });



    //realiza busca no twitter pelas hastags e grava na base de dados
    app.get('/armazena', function(req, res) 
    {

        //lê array com hashtags configuradas
        var hashtags = ['#olympics'];//config.twitter.HASHTAGS;
        //cria objeto do twitter
        var client = new Twitter({
            consumer_key: config.twitter.CONSUMER_KEY,
            consumer_secret: config.twitter.CONSUMER_SECRET,
            access_token_key: config.twitter.ACCESS_TOKEN_KEY,
            access_token_secret: config.twitter.ACCESS_TOKEN_SECRET
        });
        var palavra_busca;
        var parametros;
        const query_insert = 'INSERT INTO tweets(uuid, hastag, user_id, name, screen_name, followers_count, lang, tweet, date, schedule, hour) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);';


        //Opções do cassandra
        var options ={
            contactPoints: [config.cassandra.CONTACTPOINTS],
            authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra','cassandra'),
            keyspace: config.cassandra.KEYSPACE
        }

        //instancia e conexão do cassandra
        client_cassandra = new cassandra.Client(options);
        client_cassandra.connect(function(err){
            if(err)
            {
                //retorna a msg de erro e também o objeto que foi enviado, para preencher novamente o formulário
                res.render('erros', {err:err, hastag:''});

                //retorno erro 400 para o usuário
                res.status(400);
                return;
                }
        });

        for(var h =0; h< hashtags.length; h++){
            palavra_busca = hashtags[h];

            //parametros para fazer a busca no twitter
            parametros ={
                q: palavra_busca, 
                count: config.twitter.COUNT,
                result_type: config.twitter.RESULT_TYPE
            };

            //realiza a pesquisa na API do twitter
            client.get('search/tweets', parametros, function(error, tweets, response) {
                if(!error){
                    var uuid, hashtag, user_id, name, screen_name, followers_count, lang, tweet, date, schedule, hour;

                    for(var i=0; i<tweets.statuses.length; i++)
                    {
                        var item = tweets.statuses[i];

                        var info = {};

                        //monta objeto com informações necessarias
                        info.uuid = Uuid.random();

                        info.hashtag = palavra_busca;
                        info.user_id = item.user.user_id;
                        info.name = item.user.name;
                        info.screen_name = item.user.screen_name;
                        info.followers_count = item.user.followers_count;
                        info.lang = item.lang;
                        info.tweet = item.text;
                        //tratamento de datas
                        var data_tweet = new Date(item.created_at)
                        info.date = ('0' + data_tweet.getDate()).slice(-2) + '/' + ('0' +(data_tweet.getMonth() + 1)).slice(-2)  + '/' + data_tweet.getFullYear();
                        info.schedule =  data_tweet.getHours() + ':' + (data_tweet.getMinutes() + 1)  + ':' + data_tweet.getSeconds();
                        info.hour = data_tweet.getHours();
                        
                        //insere linha no cassandra

                        var params = [info.uuid, info.hashtag, info.user_id, info.name, info.screen_name, info.followers_count, info.lang, info.tweet, info.date, info.schedule, info.hour];
                        client_cassandra.execute(query_insert, params, {prepare:true}, function(erro_query)
                        {
                            if(erro_query)
                            {
                                //retorna a msg de erro e também o objeto que foi enviado, para preencher novamente o formulário
                                res.render('erros', {err:erro_query, hastag:''});

                                //retorno erro 400 para o usuário
                                res.status(400);
                                return;
                            }
                        });
                    }
                }
            });

        };

        //finaliza conexão do cassandra
        client_cassandra.shutdown();

        //retorna em html ou JSON
        res.format({
            json:function(){
                    //retorno JSON
                    res.json(resultado);
                },
                //retorno HTML
                html:function()
                {
                    res.render('pesquisa_realizada');
                }
            });
    });



    //realiza busca no twitter pelas hastags e grava na base de dados
    app.get('/tweets/lista', function(req, res) 
    {

        var parametros;
        var resposta = [];
        //instancia cassandra
        var options ={
            contactPoints: [config.cassandra.CONTACTPOINTS],
            authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra','cassandra'),
            keyspace: config.cassandra.KEYSPACE
        }

        client_cassandra = new cassandra.Client(options);
        client_cassandra.connect(function(err){
            if(err){
                //retorna a msg de erro e também o objeto que foi enviado, para preencher novamente o formulário
                res.render('erros', {err:err});

                //retorno erro 400 para o usuário
                res.status(400);
                return;
            }
        });


        //consulta
        const query_select = 'select uuid, hastag, user_id, name, screen_name, followers_count, lang, tweet, date , schedule, hour from tweets;';
        client_cassandra.execute(query_select, { preprare: true }, function(erro_query, resultado){
            if(!erro_query){

                //retorna em html ou JSON
                res.format({
                    json:function(){
                        //retorno JSON
                        res.json(resposta);
                    },
                    //retorno HTML
                    html:function(){
                        res.render('tweets',{tweets:resultado.rows});
                    }
                });
                }
        });

        //finaliza conexão do cassandra
        client_cassandra.shutdown();
    });



    //lista os 5 seguidores com maior quantidade de seguidores
    app.get('/lista/seguidores', function(req, res) 
    {

        var parametros;
        var resposta = [];
        //instancia cassandra
        var options ={
            contactPoints: [config.cassandra.CONTACTPOINTS],
            authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra','cassandra'),
            keyspace: config.cassandra.KEYSPACE
        }

        client_cassandra = new cassandra.Client(options);
        client_cassandra.connect(function(err){
            if(err){
                //retorna a msg de erro e também o objeto que foi enviado, para preencher novamente o formulário
                res.render('erros', {err:err});

                //retorno erro 400 para o usuário
                res.status(400);
                return;
            }
        });

        var ordenado = [];
        var aux = [];

        //consulta
        const query_select = "select  name, screen_name, followers_count, lang, hastag from tweets;";
        client_cassandra.execute(query_select, { preprare: true }, function(erro_query, resultado){
            if(!erro_query){

                aux = resultado.rows;

                //função para ordenar array pelo campo followers_count
                var sort_by = function(field, reverse, primer){

                   var key = primer ? 
                       function(x) {return primer(x[field])} : 
                       function(x) {return x[field]};

                   reverse = !reverse ? 1 : -1;

                   return function (a, b) {
                       return a = key(a), b = key(b), reverse * ((a > b) - (b > a));
                     } 
                }

                // Sort by price high to low
                ord = aux.sort(sort_by('followers_count', true, parseInt));           

                for(var i = 0; i < 5; i++ ){
                    ordenado.push(ord[i]);
                }



                //retorna em html ou JSON
                res.format({
                    json:function(){
                        //retorno JSON
                        res.json(resposta);
                    },
                    //retorno HTML
                    html:function(){
                        res.render('top_5',{tweets:ordenado});
                    }
                });
                }
        });

        //finaliza conexão do cassandra
        client_cassandra.shutdown();
    });



    //soma a quantidade de hastags existentes na base de dados por lang = pt
    app.get('/lista/hashtag', function(req, res) 
    {

        var parametros;
        var resposta = [];
        //instancia cassandra
        var options ={
            contactPoints: [config.cassandra.CONTACTPOINTS],
            authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra','cassandra'),
            keyspace: config.cassandra.KEYSPACE
        }

        client_cassandra = new cassandra.Client(options);
        client_cassandra.connect(function(err){
            if(err){
                //retorna a msg de erro e também o objeto que foi enviado, para preencher novamente o formulário
                res.render('erros', {err:err});

                //retorno erro 400 para o usuário
                res.status(400);
                return;
            }
        });

        var ordenado = [];

        //consulta na base
        const query_select = "select  name, screen_name, followers_count, lang, hastag from tweets;";
        client_cassandra.execute(query_select, { preprare: true }, function(erro_query, resultado){
            if(!erro_query){

                ordenado = resultado.rows;
                sumario = {};

                for(var i = 0; i < ordenado.length; i++){
                    var item = ordenado[i];

                    if(item.lang == "pt"){
                        if(sumario[item.hastag] == null){
                            sumario[item.hastag] = 1;
                        }
                        else
                        {
                            sumario[item.hastag] = sumario[item.hastag]+1;
                        }
                    }
                }

                //ordenado = sumario;


                var retorno = [];

                // transofrma o objeto
                forEach(sumario, function (value, key) {
                    retorno.push(
                    {
                        hashtag: key,
                        total: value,

                    });
                });


                //retorna em html ou JSON
                res.format({
                    json:function(){
                        //retorno JSON
                        res.json(resposta);
                    },
                    //retorno HTML
                    html:function(){
                        res.render('hashtag_pt',{pesquisa:retorno});
                    }
                });
                }
        });

        //finaliza conexão do cassandra
        client_cassandra.shutdown();
    });



        //soma a quantidade de hastags existentes na base de dados por lang = pt
    app.get('/lista/hora', function(req, res) 
    {

        var parametros;
        var resposta = [];
        //instancia cassandra
        var options ={
            contactPoints: [config.cassandra.CONTACTPOINTS],
            authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra','cassandra'),
            keyspace: config.cassandra.KEYSPACE
        }

        client_cassandra = new cassandra.Client(options);
        client_cassandra.connect(function(err){
            if(err){
                //retorna a msg de erro e também o objeto que foi enviado, para preencher novamente o formulário
                res.render('erros', {err:err});

                //retorno erro 400 para o usuário
                res.status(400);
                return;
            }
        });

        var ordenado = [];

        //consulta na base
        const query_select = "select  name, screen_name, followers_count, lang, hastag, hour from tweets;";
        client_cassandra.execute(query_select, { preprare: true }, function(erro_query, resultado){
            if(!erro_query){

                ordenado = resultado.rows;
                sumario = {};

                for(var i = 0; i < ordenado.length; i++){
                    var item = ordenado[i];
                    if(sumario[item.hour] == null){
                        sumario[item.hour] = 1;
                    }
                    else
                    {
                        sumario[item.hour] = sumario[item.hour]+1;
                    }
                }


                var retorno = [];

                // transofrma o objeto
                forEach(sumario, function (value, key) {
                    retorno.push(
                    {
                        hashtag: key,
                        total: value,

                    });
                });


                //retorna em html ou JSON
                res.format({
                    json:function(){
                        //retorno JSON
                        res.json(resposta);
                    },
                    //retorno HTML
                    html:function(){
                        res.render('hashtag_pt',{pesquisa:retorno});
                    }
                });
                }
        });

        //finaliza conexão do cassandra
        client_cassandra.shutdown();
    });



    //função de apoio
    function forEach(collection, callBack) {
        var
            i = 0, // Array and string iteration
            iMax = 0, // Collection length storage for loop initialisation
            key = '', // Object iteration
            collectionType = '';

        // Verify that callBack is a function
        if (typeof callBack !== 'function') {
            throw new TypeError("forEach: callBack should be function, " + typeof callBack + "given.");
        }

        // Find out whether collection is array, string or object
        switch (Object.prototype.toString.call(collection)) {
        case "[object Array]":
            collectionType = 'array';
            break;

        case "[object Object]":
            collectionType = 'object';
            break;

        case "[object String]":
            collectionType = 'string';
            break;

        default:
            collectionType = Object.prototype.toString.call(collection);
            throw new TypeError("forEach: collection should be array, object or string, " + collectionType + " given.");
        }

        switch (collectionType) {
        case "array":
            for (i = 0, iMax = collection.length; i < iMax; i += 1) {
                callBack(collection[i], i);
            }
            break;

        case "string":
            for (i = 0, iMax = collection.length; i < iMax; i += 1) {
                callBack(collection.charAt(i), i);
            }
            break;

        case "object":
            for (key in collection) {
                // Omit prototype chain properties and methods
                if (collection.hasOwnProperty(key)) {
                    callBack(collection[key], key);
                }
            }
            break;

        default:
            throw new Error("Continuity error in forEach, this should not be possible.");
        }

        return null;
    }    
};