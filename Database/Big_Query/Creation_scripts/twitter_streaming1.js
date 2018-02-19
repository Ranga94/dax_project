const BigQuery = require('@google-cloud/bigquery');

//Create a client
const bigquery = new BigQuery({
        projectId:'igenie-project',
});


var format = require('string-format')
var TwitterStreamChannels = require('twitter-stream-channels');
var credentials = require('./twitter_credentials.json');

var client = new TwitterStreamChannels(credentials);
var channels = {
	"continental" : ['CONTINENTAL AG ','Continental','DE0005439004','$CON'],
	"daimler" : ['DAIMLER AG','Daimler','DE0007100000','$DAI'],
	"deutsche_telekom" : ['DEUTSCHE TELEKOM AG ','Deutsche Telekom','DE0005557508','$DTE'],
	"fresenius" : ['FRESENIUS SE & CO. KGAA','Fresenius','DE0005785604','$FRE'],
	"heidelbergcement" : ['HEIDELBERGCEMENT Ag','HeidelbergCement','DE0006047004','$HEI'],
	"henkel" : ['HENKEL AG & CO. KGAA',' Henkel','DE0006048432','$HEN3'],
	"infineon":['INFINEON TECHNOLOGIES AG','Infineon','DE0006231004','$IFX'],
	"lufthansa":['DEUTSCHE LUFTHANSA AG','Lufthansa','DE0008232125','$LHA'],
	"mrga" : ['MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN','Münchener Rückversicherungs-Gesellschaft','DE0008430026','$MUV2'],
	"prosiebensat" : ['PROSIEBENSAT.1 MEDIA SE','ProSiebenSat1 Media','DE000PSM7770',' $PSM']
};

var stream = client.streamChannels({track:channels});

stream.on('channels/continental',function(tweet){
    console.log(tweet.text);//any tweet with 'continental'
    var constituent = "CONTINENTAL AG"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/daimler',function(tweet){
    console.log(tweet.text);//any tweet with 'daimler'
    var constituent = "DAIMLER AG"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/deutsche_telekom',function(tweet){
    console.log(tweet.text);//any tweet with 'deutsche_telekom'
    var constituent = "DEUTSCHE TELEKOM"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/fresenius',function(tweet){
    console.log(tweet.text);//any tweet with 'fresenius'
    var constituent = "FRESENIUS"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/heidelbergcement',function(tweet){
    console.log(tweet.text);//any tweet with 'heidelbergcement'
    var constituent = "HEIDELBERGCEMENT Ag"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/henkel',function(tweet){
    console.log(tweet.text);//any tweet with 'henkel'
    var constituent = "HENKEL AG & CO. KGAA"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/infineon',function(tweet){
    console.log(tweet.text);//any tweet with 'infineon'
    var constituent = "INFINEON TECHNOLOGIES AG"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/lufthansa',function(tweet){
    console.log(tweet.text);//any tweet with 'lufthansa'
    var constituent = "DEUTSCHE LUFTHANSA AG"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/mrga',function(tweet){
    console.log(tweet.text);//any tweet with 'mrga'
    var constituent = "Münchener Rückversicherungs-Gesellschaft"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});

stream.on('channels/prosiebensat',function(tweet){
    console.log(tweet.text);//any tweet with 'prosiebensat'
    var constituent = "PROSIEBENSAT"
    const sqlQuery = 'INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter` (constituent,text,name,screen_name,location,followers_count,created_at,reply_count,retweet_count) VALUES("'+constituent+'","'+tweet.text+'","'+tweet.user.name+'","'+tweet.user.screen_name+'","'+tweet.user.location+'",'+tweet.user.followers_count+',"'+tweet.created_at+'",'+tweet.reply_count+','+tweet.retweet_count+')'
const options = {
  query: sqlQuery,
  useLegacySql: false, // Use standard SQL syntax for queries.
};
bigquery
  .query(options)
  .then(results => {
    const rows = results[0];
    printResult(rows);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

});


	