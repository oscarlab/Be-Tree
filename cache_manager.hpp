#ifndef CACHE_MANAGER_HPP
#define CACHE_MANAGER_HPP

#include <set>
#include <stdint.h>

// Extend this class for the references you want to keep in the cache
template<class CacheManager>
class reference_to_cacheable_object {
public:
	virtual reference_to_cacheable_object<CacheManager> & get_write_unit_ref(void) = 0;
	virtual typename CacheManager::access_info          & get_access_info(void) = 0;
	virtual bool                                          is_dirty(void) const = 0;
	virtual bool                                          is_pinned(void) const = 0;
	
	virtual void clean(void) = 0;
	virtual void evict(void) = 0;
};

class lru_cache_manager {
public:
	lru_cache_manager(uint64_t cache_size);

	void set_cache_size(uint64_t sz);

	typedef uint64_t access_info;
	typedef reference_to_cacheable_object<lru_cache_manager> reference;

	// Notify the cache manager of a birth, load, read, write, cleaning, or
	// eviction of the given read_unit_ref.
	void note_birth(reference & read_unit_ref); // The object was created
	void note_load(reference & read_unit_ref);  // The object was loaded into cache
	void note_read(reference & read_unit_ref);  // The in-cache object was examined
	void note_write(reference & read_unit_ref); // The in-cache object was modified
	void note_clean(reference & read_unit_ref); // The in-cache object was made durable
	void note_evict(reference & read_unit_ref); // The object was evicted from cache
	void note_death(reference & read_unit_ref); // The object was destroyed
	
private:
	void maybe_evict_something(void);

	// This LRU implementation does not distinguish between births and
	// loads or between reads and writes, so handle both with this
	// function.
 	void note_birth_or_load(reference & read_unit_ref);
	void note_read_or_write(reference & read_unit_ref); 
	
	static bool compare_reference_by_access_time(reference *, reference *);
	
  std::set<reference *, bool (*)(reference *, reference *)> cache;
  uint64_t max_in_memory_objects;
	uint64_t update_interval;
  uint64_t next_access_time;
};

#endif // CACHE_MANAGER_HPP
