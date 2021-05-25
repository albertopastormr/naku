from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def hello():
    
    rdd = sc.parallelize(['Hello,', 'world!'])
    words = sorted(rdd.collect())
    print(words)

def main():
    sc = SparkContext(appName="Naku-Dataproc")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 60)
    ps_stream = KafkaUtils.createStream(ssc, 'cdh57-01-node-01.moffatt.me:2181', 'spark-streaming', {'twitter':1})

    count_this_batch = ps_stream.count().map(lambda x:('Tweets this batch: %s' % x))

    count_this_batch.pprint()
    
    ssc.start()
    ssc.awaitTermination()

if __name__ =="__main__":
    main()