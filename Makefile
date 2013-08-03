all:
	gcc -g -ggdb3 -Wall -Werror -pedantic -I. -I/usr/include/postgresql pgq.c test/consumer.c -o consumer -lpq -lpgtypes
	gcc -g -ggdb3 -Wall -Werror -pedantic -I. -I/usr/include/postgresql pgq.c test/producer.c -o producer -lpq -lpgtypes

clean:
	rm -f consumer producer
