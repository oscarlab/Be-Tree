CXXFLAGS=-Wall -std=c++11 -g -O3 
#CXXFLAGS=-Wall -std=c++11 -g -pg
#CXXFLAGS=-Wall -std=c++11 -g -pg -DDEBUG
CC=g++

test: test.cpp betree.hpp swap_space.o backing_store.o

swap_space.o: swap_space.cpp swap_space.hpp backing_store.hpp

backing_store.o: backing_store.hpp backing_store.cpp

clean:
	$(RM) *.o test
