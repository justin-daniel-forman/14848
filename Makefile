all:
	g++ -Wall -Werror -g -std=c++11 -o col test.cpp column.cpp db.cpp
clean:
	rm ./col
