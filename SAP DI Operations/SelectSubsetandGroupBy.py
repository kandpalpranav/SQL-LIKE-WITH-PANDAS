import  pandas as pd
from io import StringIO
from hdfs import InsecureClient , HdfsError
client = InsecureClient('http://datalake:50070')

def read_file(msg):
    
    attr = dict()
    api.send("outData",api.Message(attributes=attr, body= 'Param Loading Triggered'))
    sdl_file = '/shared/SLT/SFLIGHT/data.csv'
    with client.read(sdl_file , encoding='utf-8') as reader:
        df = pd.read_csv(reader)
        df = df[['CARRID' , 'CONNID' , 'PAYMENTSUM']]
        df = df.groupby(['CARRID','CONNID']).agg('sum')
        api.send("outData",api.Message(attributes=attr, body= str(df)))
        api.send("out2",str(df))

api.set_port_callback("input", read_file)
