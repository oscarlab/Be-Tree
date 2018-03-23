#include <iostream>
#include <stddef.h>
#include <assert.h>
#include "cache_manager.hpp"
#include "debug.hpp"

lru_cache_manager::lru_cache_manager(uint64_t cache_size)
	: cache(compare_reference_by_access_time),
		max_in_memory_objects(cache_size),
		update_interval(max_in_memory_objects/100),
		next_access_time(update_interval+1)
{}

lru_cache_manager::~lru_cache_manager(void) {
	set_cache_size(0);
}

void lru_cache_manager::set_cache_size(uint64_t sz) {
	max_in_memory_objects = sz;
	maybe_evict_something();
}

void lru_cache_manager::note_birth_or_load(lru_cache_manager::reference & read_unit_ref) {
	lru_cache_manager::access_info & ruai = read_unit_ref.get_access_info();
	ruai = next_access_time++;
	cache.insert(&read_unit_ref);
	maybe_evict_something();
}

void lru_cache_manager::note_birth(lru_cache_manager::reference & read_unit_ref) {
	debug(std::cout << "BIRTH of " << &read_unit_ref << std::endl);
	note_birth_or_load(read_unit_ref);
}

void lru_cache_manager::note_load(lru_cache_manager::reference & read_unit_ref) {
	debug(std::cout << "LOAD of " << &read_unit_ref << std::endl);
	note_birth_or_load(read_unit_ref);
}

void lru_cache_manager::note_read_or_write(lru_cache_manager::reference & read_unit_ref) {
	lru_cache_manager::reference & write_unit_ref = read_unit_ref.get_write_unit_ref();
	lru_cache_manager::access_info & ruai = read_unit_ref.get_access_info();
	lru_cache_manager::access_info & wuai = write_unit_ref.get_access_info();

	if (next_access_time - ruai > update_interval) {
		cache.erase(&read_unit_ref);
		ruai = next_access_time++;
		cache.insert(&read_unit_ref);
		if (&write_unit_ref != &read_unit_ref) {
			cache.erase(&write_unit_ref);
			wuai = ruai;
			cache.insert(&write_unit_ref);
		}
	}
}

void lru_cache_manager::note_read(lru_cache_manager::reference & read_unit_ref) {
	debug(std::cout << "READ of " << &read_unit_ref << std::endl);
	note_read_or_write(read_unit_ref);
}

void lru_cache_manager::note_write(lru_cache_manager::reference & read_unit_ref) {
	debug(std::cout << "WRITE of " << &read_unit_ref << std::endl);
	note_read_or_write(read_unit_ref);
}

void lru_cache_manager::note_clean(lru_cache_manager::reference & read_unit_ref) {
	debug(std::cout << "CLEAN of " << &read_unit_ref << std::endl);
	// This LRU implementation doesn't care
}

void lru_cache_manager::note_evict(lru_cache_manager::reference & read_unit_ref) {
	debug(std::cout << "EVICT of " << &read_unit_ref << std::endl);
	cache.erase(&read_unit_ref);
}

void lru_cache_manager::note_death(lru_cache_manager::reference & read_unit_ref) {
	debug(std::cout << "DEATH of " << &read_unit_ref << std::endl);
	cache.erase(&read_unit_ref);
}

void lru_cache_manager::maybe_evict_something(void) {
	while (cache.size() > max_in_memory_objects) {
		reference_to_cacheable_object<lru_cache_manager> * best = NULL;
		
		for (auto * ref : cache) {
			if (ref->is_pinned())
				continue;
			
			lru_cache_manager::reference & write_unit_ref = ref->get_write_unit_ref();
			if (ref != &write_unit_ref && write_unit_ref.is_dirty()) {
				continue;
			}

			best = ref;
			break;
		}

		if (best == NULL)
			return;
		
		if (best->is_dirty())
			best->clean();
		best->evict();
	}
}

void lru_cache_manager::checkpoint(void) {
	for (auto * ref : cache) {
		lru_cache_manager::reference & write_unit_ref = ref->get_write_unit_ref();
		if (write_unit_ref.is_dirty())
			write_unit_ref.clean();
	}
}

bool lru_cache_manager::compare_reference_by_access_time(lru_cache_manager::reference * a ,
																												 lru_cache_manager::reference * b) {
	access_info &aai = a->get_access_info();
	access_info &bai = b->get_access_info();
	return aai < bai;
}
	
