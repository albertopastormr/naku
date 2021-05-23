import pyspark


if __name__ =="__main__":
    sc = pyspark.SparkContext()
    rdd = sc.parallelize(['Hello,', 'world!'])
    words = sorted(rdd.collect())
    print(words)