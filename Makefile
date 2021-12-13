Server:clean
	javac -cp \* MyServer.java
	java -cp .:\* MyServer

r:
	java -cp .:\* MyServer

clean:
	rm -f *.class

