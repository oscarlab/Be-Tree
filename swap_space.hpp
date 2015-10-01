// A scheme for transparently swapping data structures in and out of
// memory.

// WARNING: this is very incomplete.  It's just enough functionality
//          for the betree.cpp.  In particular, the current system
//          does not handle cycles in the pointer graph or pointers
//          into the middle of objects (such as into an array).

// The goal of this code is to enable users to write complex in-memory
// data structures and have a separate layer (i.e. this code) manage
// I/O.  Users should be able to define their data structures as they
// see fit (i.e. they can use pointers, etc) but still control the
// granularity at which items are swapped to/from memory.

// Therefore, we define a swap_space::pointer type that represents a
// pointer from one swappable unit to another.  When the swapper elects
// to swap out an object X, it will swap out all the objects that X
// points to through regular C++ pointers.  All these objects will be
// written to a single place on the backing store, so this will be
// I/O-efficient.  The swapper does not traverse swap_space::pointers
// -- they point to separate things that should be swapped out
// independently of the thing pointing to them.

// The betree code provides an example of how this is used.  We want
// each node to be swapped in/out as a single unit, but separate nodes
// in the tree should be able to be swapped in/out independently of
// eachother.  Therefore, nodes use swap_space::pointers to point to
// eachother.  They use regular C++ pointers to point to internal
// items that should be serialized as part of the node.

// The swap_space needs to manage all pointers to swappable objects.
// New swappable objects should be created like this:
//      swap_space ss;
//      swap_space::pointer<T> p = ss.allocate(new T(constructor args));

// You can then use operator-> as normal, e.g.
//      p->some_field
//      p->some_method(args)
// Although no operator* is not defined, it should be straightforward
// to do so.

// Invoking p->some_method() pins the object referred to by p in
// memory.  Thus, during the execution of some_method(), it is safe to
// dereference "this" and any other plain C++ pointers in the object.

// Objects are automatically garbage collected.  The garbage collector
// uses reference counting.

// The current system uses LRU to select items to swap.  The swap
// space has a user-specified in-memory cache size it.  The cache size
// can be adjusted dynamically.

// Don't try to get your hands on an unwrapped pointer to the object
// or anything that is swapped in/out as part of the object.  It can
// only lead to trouble.  Casting is also probably a bad idea.  Just
// write nice, clean, type-safe, well-encapsulated code and everything
// should work just fine.

// Objects managed by this system must be sub-types of class
// serializable.  This basically defines two methods for serializing
// and deserializing the object.  See the betree for examples of
// implementing these methods.  We provide default implementations for
// a few basic types and STL containers.  Feel free to add more and
// submit patches as you need them.

// The current implementation serializes to a textual file format.
// This is just a convenience.  It would be nice to be able to swap in
// different formats.

#ifndef SWAP_SPACE_HPP
#define SWAP_SPACE_HPP

#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <functional>
#include <sstream>
#include <cassert>
#include "backing_store.hpp"
#include "debug.hpp"

class swap_space;

class serialization_context {
public:
  serialization_context(swap_space &sspace) :
    ss(sspace),
    is_leaf(true)
  {}
  swap_space &ss;
  bool is_leaf;
};

class serializable {
public:
  virtual void _serialize(std::iostream &fs, serialization_context &context) = 0;
  virtual void _deserialize(std::iostream &fs, serialization_context &context) = 0;
  virtual ~serializable(void) {};
};

void serialize(std::iostream &fs, serialization_context &context, uint64_t x);
void deserialize(std::iostream &fs, serialization_context &context, uint64_t &x);

void serialize(std::iostream &fs, serialization_context &context, int64_t x);
void deserialize(std::iostream &fs, serialization_context &context, int64_t &x);

void serialize(std::iostream &fs, serialization_context &context, std::string x);
void deserialize(std::iostream &fs, serialization_context &context, std::string &x);

template<class Key, class Value> void serialize(std::iostream &fs,
						serialization_context &context,
						std::map<Key, Value> &mp)
{
  fs << "map " << mp.size() << " {" << std::endl;
  assert(fs.good());
  for (auto it = mp.begin(); it != mp.end(); ++it) {
    fs << "  ";
    serialize(fs, context, it->first);
    fs << " -> ";
    serialize(fs, context, it->second);
    fs << std::endl;
  }
  fs << "}" << std::endl;
}

template<class Key, class Value> void deserialize(std::iostream &fs,
						  serialization_context &context,
						  std::map<Key, Value> &mp)
{
  std::string dummy;
  int size = 0;
  fs >> dummy >> size >> dummy;
  assert(fs.good());
  for (int i = 0; i < size; i++) {
    Key k;
    Value v;
    deserialize(fs, context, k);
    fs >> dummy;
    deserialize(fs, context, v);
    mp[k] = v;
  }
  fs >> dummy;
}

template<class X> void serialize(std::iostream &fs, serialization_context &context, X *&x)
{
  fs << "pointer ";
  serialize(fs, context, *x);
}

template<class X> void deserialize(std::iostream &fs, serialization_context &context, X *&x)
{
  std::string dummy;
  x = new X;
  fs >> dummy;
  assert (dummy == "pointer");
  deserialize(fs, context, *x);
}

template<class X> void serialize(std::iostream &fs, serialization_context &context, X &x)
{
  x._serialize(fs, context);
}

template<class X> void deserialize(std::iostream &fs, serialization_context &context, X &x)
{
  x._deserialize(fs, context);
}

class swap_space {
public:
  swap_space(backing_store *bs, uint64_t n);

  template<class Referent> class pointer;

  template<class Referent>
  pointer<Referent> allocate(Referent * tgt) {
    return pointer<Referent>(this, tgt);
  }

  // This pins an object in memory for the duration of a member
  // access.  It's sort of an instance of the "resource aquisition is
  // initialization" paradigm.
  template<class Referent>
  class pin {
  public:
    const Referent * operator->(void) const {
      assert(ss->objects.count(target) > 0);
      debug(std::cout << "Accessing (constly) " << target
	    << " (" << ss->objects[target]->target << ")" << std::endl);
      access(target, false);
      return (const Referent *)ss->objects[target]->target;
    }

    Referent * operator->(void) {
      assert(ss->objects.count(target) > 0);
      debug(std::cout << "Accessing " << target
	    << " (" << ss->objects[target]->target << ")" << std::endl);
      access(target, true);
      return (Referent *)ss->objects[target]->target;
    }

    pin(const pointer<Referent> *p)
      : ss(NULL),
	target(0)
    {
      dopin(p->ss, p->target);
    }

    pin(void)
      : ss(NULL),
	target(0)
    {}

    ~pin(void) {
      unpin();
    }

    pin &operator=(const pin &other) {
      if (&other != this) {
	unpin();
	dopin(other.ss, other.target);
      }
    }
    
  private:
    void unpin(void) {
      debug(std::cout << "Unpinning " << target
	    << " (" << ss->objects[target]->target << ")" << std::endl);
      if (target > 0) {
	assert(ss->objects.count(target) > 0);
	ss->objects[target]->pincount--;
	ss->maybe_evict_something();
      }
      ss = NULL;
      target = 0;
    }

    void dopin(swap_space *newss, uint64_t newtarget) {
      assert(ss == NULL && target == 0);
      ss = newss;
      target = newtarget;
      if (target > 0) {
	assert(ss->objects.count(target) > 0);
	debug(std::cout << "Pinning " << target
	      << " (" << ss->objects[target]->target << ")" << std::endl);
	ss->objects[target]->pincount++;
      }
    }
    
    void access(uint64_t tgt, bool dirty) const {
      assert(ss->objects.count(tgt) > 0);
      object *obj = ss->objects[tgt];
      ss->lru_pqueue.erase(obj);
      obj->last_access = ss->next_access_time++;
      ss->lru_pqueue.insert(obj);
      obj->target_is_dirty |= dirty;
      ss->load<Referent>(tgt);
      ss->maybe_evict_something();
    }
  
    swap_space *ss;
    uint64_t target;
  };
  
  template<class Referent>
  class pointer : public serializable {
    friend class swap_space;
    friend class pin<Referent>;
    
  public:
    pointer(void) :
      ss(NULL),
      target(0)
    {}
    
    pointer(const pointer &other) {
      ss = other.ss;
      target = other.target;
      if (target > 0) {
	assert(ss->objects.count(target) > 0);
	ss->objects[target]->refcount++;
      }
    }

    ~pointer(void) {
      depoint();
    }

    void depoint(void) {
      if (target == 0)
	return;
      assert(ss->objects.count(target) > 0);

      object *obj = ss->objects[target];
      assert(obj->refcount > 0);
      if ((--obj->refcount) == 0) {
	debug(std::cout << "Erasing " << target << std::endl);
	// Load it into memory so we can recursively free stuff
	if (obj->target == NULL) {
	  assert(obj->bsid > 0);
	  if (!obj->is_leaf) {
	    ss->load<Referent>(target);
	  } else {
	    debug(std::cout << "Skipping load of leaf " << target << std::endl);
	  }
	}
	ss->objects.erase(target);
	ss->lru_pqueue.erase(obj);
	if (obj->target)
	  delete obj->target;
	ss->current_in_memory_objects--;
	if (obj->bsid > 0)
	  ss->backstore->deallocate(obj->bsid);
	delete obj;
      }
      target = 0;
    }

    pointer & operator=(const pointer &other) {
      if (&other != this) {
	depoint();
	ss = other.ss;
	target = other.target;
	if (target > 0) {
	  assert(ss->objects.count(target) > 0);
	  ss->objects[target]->refcount++;
	}
      }
      return *this;
    }

    bool operator==(const pointer &other) const {
      return ss == other.ss && target == other.target;
    }

    bool operator!=(const pointer &other) const {
      return !operator==(other);
    }
	  
    // const Referent * operator->(void) const {
    //   ss->access(target, false);
    //   return ss->objects[target].target;
    // }

    const pin<Referent> operator->(void) const {
      return pin<Referent>(this);
    }

    pin<Referent> operator->(void) {
      return pin<Referent>(this);
    }

    pin<Referent> get_pin(void) {
      return pin<Referent>(this);
    }
    
    const pin<Referent> get_pin(void) const {
      return pin<Referent>(this);
    }
    
    bool is_in_memory(void) const {
      assert(ss->objects.count(target) > 0);
      return target > 0 && ss->objects[target]->target != NULL;
    }

    bool is_dirty(void) const {
      assert(ss->objects.count(target) > 0);
      return target > 0 && ss->objects[target]->target && ss->objects[target]->target_is_dirty;
    }

    void _serialize(std::iostream &fs, serialization_context &context) {
      assert(target > 0);
      assert(context.ss.objects.count(target) > 0);
      fs << target << " ";
      target = 0;
      assert(fs.good());
      context.is_leaf = false;
    }
    
    void _deserialize(std::iostream &fs, serialization_context &context) {
      assert(target == 0);
      ss = &context.ss;
      fs >> target;
      assert(fs.good());
      assert(context.ss.objects.count(target) > 0);
      // We just created a new reference to this object and
      // invalidated the on-disk reference, so the total refcount
      // stays the same.
    }

  private:
    swap_space *ss;
    uint64_t target;

    // Only callable through swap_space::allocate(...)
    pointer(swap_space *sspace, Referent *tgt)
    {
      ss = sspace;
      target = sspace->next_id++;

      object *o = new object(sspace, tgt);
      assert(o != NULL);
      target = o->id;
      assert(ss->objects.count(target) == 0);
      ss->objects[target] = o;
      ss->lru_pqueue.insert(o);
      ss->current_in_memory_objects++;
      ss->maybe_evict_something();
    }

  };
  
private:
  backing_store *backstore;  

  uint64_t next_id = 1;
  uint64_t next_access_time = 0;
  
  class object {
  public:
    
    object(swap_space *sspace, serializable * tgt);
    
    serializable * target;
    uint64_t id;
    uint64_t bsid;
    bool is_leaf;
    uint64_t refcount;
    uint64_t last_access;
    bool target_is_dirty;
    uint64_t pincount;
  };

  static bool cmp_by_last_access(object *a, object *b);

  template<class Referent>
  void load(uint64_t tgt) {
    assert(objects.count(tgt) > 0);
    if (objects[tgt]->target == NULL) {
      object *obj = objects[tgt];
      debug(std::cout << "Loading " << obj->id << std::endl);
      std::iostream *in = backstore->get(obj->bsid);
      Referent *r = new Referent();
      serialization_context ctxt(*this);
      deserialize(*in, ctxt, *r);
      backstore->put(in);
      obj->target = r;
      current_in_memory_objects++;
    }
  }

  void set_cache_size(uint64_t sz);
  
  void write_back(object *obj);
  void maybe_evict_something(void);
  
  uint64_t max_in_memory_objects;
  uint64_t current_in_memory_objects = 0;
  std::unordered_map<uint64_t, object *> objects;
  std::set<object *, bool (*)(object *, object *)> lru_pqueue;
};

#endif // SWAP_SPACE_HPP
