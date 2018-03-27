CXXFLAGS=-Wall -std=c++11 -g -O3 
#CXXFLAGS=-Wall -std=c++11 -g -pg
#CXXFLAGS=-Wall -std=c++17 -g -pg -no-pie -DDEBUG

LDFLAGS=-lboost_serialization

CXX=g++
#CXX=clang++

test: test.cpp betree.hpp swap_space.hpp cache_manager.o backing_store.o

cache_manager.o: cache_manager.cpp cache_manager.hpp

backing_store.o: backing_store.hpp backing_store.cpp

clean:
	$(RM) *.o test
