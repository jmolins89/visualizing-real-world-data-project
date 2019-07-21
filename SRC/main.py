import pipeline


if __name__=="__main__":
	df = pipeline.extracting('mongodb://localhost:27017/',2005)
	df = pipeline.transforming(df,1000000,2010,1/3,1/3,1/3,55)
    df = loading(df)