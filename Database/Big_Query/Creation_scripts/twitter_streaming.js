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
        "adidas" : ['ADIDAS AG','Adidas','DE000A1EWWW0','$ADS'],
		"allianz" :['ALLIANZ SE ','Allianz','DE0008404005','$ALV'],
		"basf" : ['BASF SE','BASF','DE000BASF111','$BAS'],
		"bayer": ['BAYER AG','Bayer','DE000BAY0017','$BAYN'],
		"beiersdorf" : ['BEIERSDORF AG','Beiersdorf','DE0005200000','$BEI'],
		"commerzbank" : ['COMMERZBANK AKTIENGESELLSCHAFT','Commerzbank','DE000CBK1001','$CBK']
        "brexit" : ['brexit'],
		"continental" : ['CONTINENTAL AG ','Continental','DE0005439004','$CON'],
		"daimler" : ['DAIMLER AG','Daimler','DE0007100000','$DAI'],
		"deutsche_boerse" : ['DEUTSCHE BOERSE AG','Deutsche Börse','DE0005810055','$DB1'],
		"deutsche_bank" : ['DEUTSCHE BANK AG','Deutsche Bank','DE0005140008','$DBK'],
		"deutsche_post" : ['DEUTSCHE POST AG','Deutsche Post','DE0005552004','$DPW'],
		"deutsche_telekom" : ['DEUTSCHE TELEKOM AG ','Deutsche Telekom','DE0005557508','$DTE'],
		"eon" : ['E.ON SE ','DE000ENAG999',' EON','$EON'],
		"fresenius_medical" : ['FRESENIUS MEDICAL CARE AG & CO. KGAA','Fresenius Medical Care','DE0005785802','$FME'],
		"fresenius" : ['FRESENIUS SE & CO. KGAA','Fresenius','DE0005785604','$FRE'],
		"heidelbergcement" : ['HEIDELBERGCEMENT Ag','HeidelbergCement','DE0006047004','$HEI'],
		"henkel" : ['HENKEL AG & CO. KGAA',' Henkel','DE0006048432','$HEN3'],
		"infineon":['INFINEON TECHNOLOGIES AG','Infineon','DE0006231004','$IFX'],
		"lufthansa":['DEUTSCHE LUFTHANSA AG','Lufthansa','DE0008232125','$LHA'],
		"linde" : ['LINDE AG','Linde','DE0006483001',' $LIN'],
		"merck" : ['MERCK KGAA','Merck','DE0006599905','$MRK'],
		"mrga" : ['MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN','Münchener Rückversicherungs-Gesellschaft','DE0008430026','$MUV2'],
		"prosiebensat" : ['PROSIEBENSAT.1 MEDIA SE','ProSiebenSat1 Media','DE000PSM7770',' $PSM'],
		"rwe" : ['RWE AG','DE0007037129','$RWE'],
		"sap" : ['SAP SE','DE0007164600','$SAP'],
		"siemens" : ['SIEMENS AG','Siemens','DE0007236101','$SIE'],
		"thyssenkrupp" : ['THYSSENKRUPP AG','thyssenkrupp','DE0007500001','$TKA'],
		"vonovia" : ['VONOVIA SE ','Vonovia','DE000A1ML7J1','$VNA'],
		"volkswagen" : ['VOLKSWAGEN AG','Volkswagen','DE0007664039','$VOW3'],
		"bmw" : ['BAYERISCHE MOTOREN WERKE AG','DE0005190003','$BMW'],
		"dax" : ['Dax'],
		"nasdaq" : ['Nasdaq'],
		"dow_jones" : ['Dow Jones'],
		"ftse": ['FTSE 100 Index']
};
var stream = client.streamChannels({track:channels});

stream.on('channels/adidas',function(tweet){
    console.log(tweet.text);//any tweet with 'adidas'
    var constituent = "ADIDAS AG"
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

stream.on('channels/brexit',function(tweet){
    console.log(tweet.text);//any tweet with 'brexit'
    var constituent = "BREXIT"
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
  })
});
stream.on('channels/bmw',function(tweet){
    console.log(tweet.text);//any tweet with 'bmw'
    var constituent = "BMW"
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
  })
});

stream.on('channels/allianz',function(tweet){
    console.log(tweet.text);//any tweet with 'allianz'
    var constituent = "ALLIANZ SE"
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

stream.on('channels/basf',function(tweet){
    console.log(tweet.text);//any tweet with 'basf'
    var constituent = "BASF SE"
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

stream.on('channels/bayer',function(tweet){
    console.log(tweet.text);//any tweet with 'bayer'
    var constituent = "BAYER AG"
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

stream.on('channels/beiersdorf',function(tweet){
    console.log(tweet.text);//any tweet with 'beiersdorf'
    var constituent = "BEIERSDORF AG"
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

stream.on('channels/commerzbank',function(tweet){
    console.log(tweet.text);//any tweet with 'commerzbank'
    var constituent = "COMMERZBANK"
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

stream.on('channels/deutsche_boerse',function(tweet){
    console.log(tweet.text);//any tweet with 'deutsche_boerse'
    var constituent = "DEUTSCHE BOERSE"
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

stream.on('channels/deutsche_bank',function(tweet){
    console.log(tweet.text);//any tweet with 'deutsche_bank'
    var constituent = "DEUTSCHE BANK"
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

stream.on('channels/deutsche_post',function(tweet){
    console.log(tweet.text);//any tweet with 'deutsche_post'
    var constituent = "DEUTSCHE POST"
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

stream.on('channels/eon',function(tweet){
    console.log(tweet.text);//any tweet with 'eon'
    var constituent = "E.ON SE"
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

stream.on('channels/fresenius_medical',function(tweet){
    console.log(tweet.text);//any tweet with 'fresenius_medical'
    var constituent = "FRESENIUS MEDICAL"
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

stream.on('channels/linde',function(tweet){
    console.log(tweet.text);//any tweet with 'linde'
    var constituent = "LINDE AG"
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

stream.on('channels/merck',function(tweet){
    console.log(tweet.text);//any tweet with 'merck'
    var constituent = "MERCK KGAA"
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

stream.on('channels/rwe',function(tweet){
    console.log(tweet.text);//any tweet with 'rwe'
    var constituent = "RWE AG"
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

stream.on('channels/sap',function(tweet){
    console.log(tweet.text);//any tweet with 'sap'
    var constituent = "SAP SE"
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

stream.on('channels/siemens',function(tweet){
    console.log(tweet.text);//any tweet with 'siemens'
    var constituent = "SIEMENS AG"
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

stream.on('channels/thyssenkrupp',function(tweet){
    console.log(tweet.text);//any tweet with 'thyssenkrupp'
    var constituent = "THYSSENKRUPP"
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

stream.on('channels/vonovia',function(tweet){
    console.log(tweet.text);//any tweet with 'vonovia'
    var constituent = "VONOVIA SE"
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

stream.on('channels/volkswagen',function(tweet){
    console.log(tweet.text);//any tweet with 'volkswagen'
    var constituent = "VOLKSWAGEN AG"
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

stream.on('channels/dax',function(tweet){
    console.log(tweet.text);//any tweet with 'dax'
    var constituent = "DAX"
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

stream.on('channels/nasdaq',function(tweet){
    console.log(tweet.text);//any tweet with 'nasdaq'
    var constituent = "Nasdaq"
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

stream.on('channels/dow_jones',function(tweet){
    console.log(tweet.text);//any tweet with 'dow_jones'
    var constituent = "DOW JONES"
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

stream.on('channels/ftse',function(tweet){
    console.log(tweet.text);//any tweet with 'ftse'
    var constituent = "FTSE 100"
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