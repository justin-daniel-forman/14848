all:
	g++ -Wall -Werror -std=c++11 -g -pthread -o out test.cpp column.cpp db.cpp
clean:
	rm ./out
	rm sst*
