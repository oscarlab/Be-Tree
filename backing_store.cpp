#include "backing_store.hpp"
#include <iostream>
#include <ext/stdio_filebuf.h>
#include <unistd.h>
#include <cassert>

/////////////////////////////////////////////////////////////
// Implementation of the one_file_per_object_backing_store //
/////////////////////////////////////////////////////////////
one_file_per_object_backing_store::one_file_per_object_backing_store(std::string rt)
  : root(rt),
    nextid(1)
{}

uint64_t one_file_per_object_backing_store::allocate(size_t n) {
  uint64_t id = nextid++;
  std::string filename = root + "/" + std::to_string(id);
  std::fstream dummy(filename, std::fstream::out);
  dummy.flush();
  assert(dummy.good());
  return id;
}

void one_file_per_object_backing_store::deallocate(uint64_t id) {
  std::string filename = root + "/" + std::to_string(id);
  assert(unlink(filename.c_str()) == 0);
}

std::iostream * one_file_per_object_backing_store::get(uint64_t id) {
  __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>;
  std::string filename = root + "/" + std::to_string(id);
  fb->open(filename, std::fstream::in | std::fstream::out);
  std::fstream *ios = new std::fstream;
  ios->std::ios::rdbuf(fb);
  ios->exceptions(std::fstream::badbit | std::fstream::failbit | std::fstream::eofbit);
  assert(ios->good());
  
  return ios;
}

void one_file_per_object_backing_store::put(std::iostream *ios)
{
  ios->flush();
  __gnu_cxx::stdio_filebuf<char> *fb = (__gnu_cxx::stdio_filebuf<char> *)ios->rdbuf();
  fsync(fb->fd());
  delete ios;
  delete fb;
}
