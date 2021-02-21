import  pandas as pd
from io import StringIO
from hdfs import InsecureClient , HdfsError
client = InsecureClient('http://datalake:50070')

def read_file(msg):
    
    attr = dict()
    sdl_file = '/shared/SLT/SFLIGHT/data.csv'
    LT_val = ['AC' ,820]
    with client.read(sdl_file , encoding='utf-8') as reader:
        df = pd.read_csv(reader)
        df_carrid =  df['CARRID'] ==  LT_val[0]
        df_connid =  df['CONNID'] ==  LT_val[1]
        df_result = df[df_carrid & df_connid ]
        api.send("out2",str(df_result))
        
api.set_port_callback("input", read_file)
