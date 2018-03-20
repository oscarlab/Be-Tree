#include "swap_space.hpp"

#if 0

bool swap_space::object::cmp_by_last_access(const swap_space::base_object *a,
																						const swap_space::base_object *b) {
  return a->last_access < b->last_access;
}

swap_space::swap_space(backing_store *bs, uint64_t n) :
  backstore(bs),
  max_in_memory_objects(n),
  objects(),
  lru_pqueue(object::cmp_by_last_access)
{}

void swap_space::register_new_object(swap_space::base_object *obj) {
	sspace->objects[obj->id] = obj;
	sspace->lru_pqueue.insert(obj);
	sspace->current_in_memory_objects++;
}

void swap_space::set_cache_size(uint64_t sz) {
  assert(sz > 0);
  max_in_memory_objects = sz;
  maybe_evict_something();
}

swap_space::object::~object(void) {
	// Load it into memory so we can recursively free stuff
	if (!is_leaf()) {
		ss->ensure_is_in_memory(this);
	} else {
		debug(std::cout << "Skipping load of leaf " << get_id() << std::endl);
	}
	if (get_target())
		delete_target(get_target());
	target = NULL;
	
	ss->objects.erase(get_id());
	ss->lru_pqueue.erase(this);
	ss->current_in_memory_objects--;
	if (get_bsid() > 0) {
		assert(!is_subobject());
		ss->backstore->deallocate(get_bsid());
	}
}

uint64_t swap_space::object::get_id(void) const {
	return id;
}

uint64_t swap_space::object::get_refcount(void) const {
	return refcount;
}

 

#endif
