// Generic interface to the disk.  Used by swap_space to store
// objects.

#ifndef BACKING_STORE_HPP
#define BACKING_STORE_HPP

#include <cstdint>
#include <cstddef>
#include <iostream>

class backing_store {
public:
  virtual uint64_t allocate(size_t n) = 0;
  virtual void deallocate(uint64_t id) = 0;
  virtual std::iostream * get(uint64_t id) = 0;
  virtual void            put(std::iostream *ios) = 0;
	virtual void            set_root(uint64_t id) = 0;
	virtual uint64_t        get_root(void) = 0;	
};

class one_file_per_object_backing_store: public backing_store {
public:
  one_file_per_object_backing_store(std::string rt);
  uint64_t	      allocate(size_t n);
  void		        deallocate(uint64_t id);
  std::iostream * get(uint64_t id);
  void            put(std::iostream *ios);
	void            set_root(uint64_t id);
	uint64_t        get_root(void);	
  
private:
  std::string	root;
  uint64_t	nextid;
};

#endif // BACKING_STORE_HPP
