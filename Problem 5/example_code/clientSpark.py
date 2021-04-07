# import socket

# if __name__ == "__main__":
#     HOST = 'localhost'
#     PORT = 12346

#     with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as client:
#         client.connect((HOST, PORT))
#         while(True):
#             data = client.recv(1024)
#             if not data:
#                 break
#             print(data.decode('utf-8'))

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    HOST = 'localhost'
    PORT = 12345

    sc = SparkContext(master="local[*]", appName="TwitterCount")
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 10)

    tag_stream = ssc.socketTextStream(hostname=HOST, port=PORT)
    tags = tag_stream.window(10)
    
    tagCount = tags.flatMap( lambda text: text.split( " " ) ).filter( lambda word: word.lower().startswith("#") ).map( lambda word: ( word.lower(), 1 ) ).reduceByKey( lambda a, b: a + b )
    # tagCount = tags.flatMap(lambda tag:tag.split(' ')).map(lambda tag:(tag,1)).reduceByKey(lambda x,y:x+y)
    
    tagCount.pprint()
    # tags.pprint()

    ssc.start()
    ssc.awaitTermination()