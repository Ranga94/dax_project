# Usage
The following scripts require installing [PhantomJS](https://www.google.co.uk/url?sa=t&rct=j&q=&esrc=s&source=web&cd=2&cad=rja&uact=8&ved=0ahUKEwiT39HNyYnVAhWsAMAKHYhyDVcQjBAILjAB&url=http%3A%2F%2Fphantomjs.org%2Fdownload.html&usg=AFQjCNEk_YHhT1IGPLBNnrLN8YctpKNtrQ) and the following python libraries:
```
pip install requests
pip install selenium
pip install beautifulsoup4
pip install pymongo
```

## Historical data extraction
_scraper.py_ is a tool to extract the historical data from DAX and its 30 constituents. It stores the data in a MongoDB database. The following is an example of a document in the _historical_ collection:
```
{
    "_id": {
        "$oid": "596905a2d387bd07c5ddf44f"
    },
    "constituent": "Daimler",
    "date": {
        "$date": "2003-02-13T00:00:00.000Z"
    },
    "opening_price": 26.9,
    "closing_price": 26.75,
    "daily_high": 27.35,
    "daily_low": 26.45,
    "turnover": 6400635,
    "volume": 239168
}
```

```
usage: scraper.py [-h] [-a | -c CONSTITUENT] connection_string

positional arguments:
  connection_string     The MongoDB connection string

optional arguments:
  -h, --help            show this help message and exit
  -a, --all             save historical data for all constituents
  -c CONSTITUENT, --constituent CONSTITUENT
                        save historical data for specific constituent
```
Example:
```
scraper.py -c 'Deutsche Bank' <some connection string>
```

## Company data
_meta-data.py_ is a tool that extracts relevant information from the 30 constituents, like dividends, earning reports, etc, and stores it in a MongoDB database.  The following is an example of a document in the _company-data_ collection:
```
{
    "_id": {
        "$oid": "59692115d387bd09e2187589"
    },
    "table": "Master Data",
    "constituent": "Allianz",
    "Transparency Level on First Quotation": "Prime Standard"
}
```

```
usage: meta-data.py [-h] [-a | -c CONSTITUENT] connection_string

positional arguments:
  connection_string     The MongoDB connection string

optional arguments:
  -h, --help            show this help message and exit
  -a, --all             save company data for all constituents
  -c CONSTITUENT, --constituent CONSTITUENT
                        save company data for a specific constituent
```
Example:
```
meta-data.py -c "BMW" <some connection string>
```

## Real-time data
_real_time.py_ is a tool that obtains the tick data for DAX or any of the 30 consittuents and appends to a collection in a MongoDB database. The following is an example of a document in the *dax_real_time* collection:
```
{
    "_id": {
        "$oid": "5968ab21d387bd0220fbf540"
    },
    "constituent": "Allianz",
    "date": "2017-07-14-12",
    "time": "23-18-00",
    "datetime": {
        "$date": "2017-07-14T12:23:18.000Z"
    },
    "price": 183.357
}
```

```
usage: real_time.py [-h] [-c CONSTITUENT] connection_string

positional arguments:
  connection_string     The MongoDB connection string

optional arguments:
  -h, --help            show this help message and exit
  -c CONSTITUENT, --constituent CONSTITUENT
                        save real-time data for specific constituent
```
Example:
```
real_time.py -c "Deutsche Telekom" <some connection string>
```
A cron job should be set up to run this script with the desired periodicity.

## Downloading the data
At the moment, the data for historical prices and company data is hosted in a mLab.com MongoDB database. To download the data, a local installation of MongoDB is required:
```
mongoexport -h ds019654.mlab.com:19654 -d dax -c [company_data | historical | real_time] -u igenie -p igenie -o <path to save file>
```
