import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import requests
import json
#{'avro.codec': b'null', 'avro.schema': b'{"type":"record","name":"EventData","namespace":"Microsoft.ServiceBus.Messaging","fields":[{"name":"SequenceNumber","type":"long"},{"name":"Offset","type":"string"},{"name":"EnqueuedTimeUtc","type":"string"},{"name":"SystemProperties","type":{"type":"map","values":["long","double","string","bytes"]}},{"name":"Properties","type":{"type":"map","values":["long","double","string","bytes","null"]}},{"name":"Body","type":["null","bytes"]}]}'}

def avro_output(link):
    try:
        print(link)
        avro_file = requests.get(link)
        print(avro_file.status_code)
        with open("avro_file_1.avro", "wb") as f:
            f.write(avro_file.content)
            f.close()
        reader = DataFileReader(open("avro_file_1.avro","rb"),avro.io.DatumReader())
        schema = reader.meta
        #print(schema)
        #avro_data = [content for content in reader]
        #print(len(avro_data))

        avro_content = [str(content["Body"]) for content in reader]
        return json.dumps({"content": avro_content[0]})   
    except:
        print("sorry, unable to fetch the avro file")
'''
    for datablock in range(len(avro_content)):
        print(avro_content[datablock].strip("b'"))
'''
