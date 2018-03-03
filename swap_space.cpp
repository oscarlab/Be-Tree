#include "swap_space.hpp"

bool swap_space::cmp_by_last_access(swap_space::object *a, swap_space::object *b) {
  return a->last_access < b->last_access;
}

swap_space::swap_space(backing_store *bs, uint64_t n) :
  backstore(bs),
  max_in_memory_objects(n),
  objects(),
  lru_pqueue(cmp_by_last_access)
{}

swap_space::object::object(swap_space *sspace, void * tgt,
													 swap_space::object_writer write_ob,
													 swap_space::object_deleter delete_ob) {
  target = tgt;
  id = sspace->next_id++;
  bsid = 0;
  is_leaf = false;
  refcount = 1;
  last_access = sspace->next_access_time++;
  target_is_dirty = true;
  pincount = 0;
	write_obj = write_ob;
	delete_obj = delete_ob;
}

void swap_space::set_cache_size(uint64_t sz) {
  assert(sz > 0);
  max_in_memory_objects = sz;
  maybe_evict_something();
}

void swap_space::write_back(swap_space::object *obj)
{
  assert(objects.count(obj->id) > 0);

  debug(std::cout << "Writing back " << obj->id
	<< " (" << obj->target << ") "
	<< "with last access time " << obj->last_access << std::endl);

  // This calls _serialize on all the pointers in this object,
  // which keeps refcounts right later on when we delete them all.
  // In the future, we may also use this to implement in-memory
  // evictions, i.e. where we first "evict" an object by
  // compressing it and keeping the compressed version in memory.
  serialization_context ctxt(this);
  std::stringstream sstream;
	oarchive_t ar(sstream);
	ar.template get_helper<serialization_context>(&ar) = ctxt;
	obj->write_obj(ar, obj->target);
  obj->is_leaf = ctxt.is_leaf;

  if (obj->target_is_dirty) {
    std::string buffer = sstream.str();
    uint64_t bsid = backstore->allocate(buffer.length());
    std::iostream *out = backstore->get(bsid);
    out->write(buffer.data(), buffer.length());
    backstore->put(out);
    if (obj->bsid > 0)
      backstore->deallocate(obj->bsid);
    obj->bsid = bsid;
    obj->target_is_dirty = false;
  }
}

void swap_space::maybe_evict_something(void)
{
  while (current_in_memory_objects > max_in_memory_objects) {
    object *obj = NULL;
    for (auto it = lru_pqueue.begin(); it != lru_pqueue.end(); ++it)
      if ((*it)->pincount == 0) {
				obj = *it;
				break;
      }
    if (obj == NULL)
      return;
    lru_pqueue.erase(obj);
		
    write_back(obj);
    
    obj->delete_obj(obj->target);
    obj->target = NULL;
    current_in_memory_objects--;
  }
}

