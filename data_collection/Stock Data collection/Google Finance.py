import numpy
import pandas
import pandas_datareader as pdr
import json

data = pdr.get_data_google('FRA:(insert ticker symbol here)')
data.to_json('insert directory of output file')
#print(data)


#print(type(data))

#with open('directory', 'a') as outfile:
    #writer = csv.writer(outfile, delimiter=',')
    #for line in data:
        #writer.writerow(line)

#with open('test.json', 'a') as outfile:
    #json.dump(data, outfile)

