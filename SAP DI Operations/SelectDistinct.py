import  pandas as pd
from io import StringIO
from hdfs import InsecureClient , HdfsError
client = InsecureClient('http://datalake:50070')

def read_file(msg):
    
    attr = dict()
    sdl_file = '/shared/SLT/SFLIGHT/data.csv'
    with client.read(sdl_file , encoding='utf-8') as reader:
        df = pd.read_csv(reader)
        LT_CARRID = df['CARRID'].unique()
        api.send("out2",str(LT_CARRID))
        api.send("out2",'Type of LT_CARRID')
        api.send("out2",str(type(LT_CARRID)))
        
api.set_port_callback("input", read_file)
