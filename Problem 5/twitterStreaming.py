from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[*]', appName='TwitterStreaming')
ssc = StreamingContext(sc, 10)

socket_stream = ssc.socketTextStream("127.0.0.1", 5555)

lines = socket_stream.window(20)

wordCounts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()