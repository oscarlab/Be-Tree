// A scheme for transparently swapping data structures in and out of
// memory.

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

// This system uses boost serialization to read and write objects on
// disk.  Thus all the objects to be swapped must be serializable
// using boost serialization.


#ifndef SWAP_SPACE_HPP
#define SWAP_SPACE_HPP

#include <cstdint>
#include <set>
#include <sstream>
#include <cassert>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include "backing_store.hpp"
#include "cache_manager.hpp"
#include "debug.hpp"

typedef boost::archive::text_oarchive oarchive_t;
typedef boost::archive::text_iarchive iarchive_t;

template <class CacheManager=lru_cache_manager>
class swap_space;

template <class CacheManager>
class base_object;

template <class CacheManager>
using refcount_map
= std::unordered_map<base_object<CacheManager> *, uint64_t>;

template <class CacheManager>
using unresolved_link_map
= std::unordered_map<uint64_t, 
                     std::unordered_map<base_object<CacheManager> *, uint64_t> >;

template <class CacheManager>
class serialization_context {
  public:
  serialization_context(void) : ss(NULL), refcounts(NULL) {}
  serialization_context(swap_space<CacheManager> *sspace, 
	                      refcount_map<CacheManager> * refcnts)
	: ss(sspace),
	refcounts(refcnts)
	{}
	swap_space<CacheManager> *ss;
	refcount_map<CacheManager> * refcounts;
};

template<class Archive, class CacheManager>
swap_space<CacheManager> * get_swap_space(Archive &ar) {
	serialization_context<CacheManager> & context
		= ar.template get_helper<serialization_context<CacheManager> >((void *)&ar);
	return ar.ss;
}

template<class CacheManager>
void dump_refmap(refcount_map<CacheManager> &rcm) {
  for (const auto & p : rcm) {
    std::cout << p.first << "(" << p.first->get_id() << "):" << p.second << " ";
  }
  std::cout << std::endl;
}

template <class CacheManager>
class checkpoint_context {
  public:
  checkpoint_context(void) : ss(NULL) {}
  checkpoint_context(swap_space<CacheManager> *sspace)
	: ss(sspace)
	{}
	swap_space<CacheManager> *ss;
	unresolved_link_map<CacheManager> unresolved_links;
};

template <class CacheManager>
class base_object : public reference_to_cacheable_object<CacheManager> {
	friend class swap_space<CacheManager>;
public:

	base_object(void) = delete;
	
	base_object(swap_space<CacheManager> *sspace,
							void *tgt)
		: id(sspace->next_id++),
			target(tgt),
			ss(sspace),
			bsid(0),
			ondisk_referents(),
			refcount(0),
			pincount(0),
			isdirty(true)
	{
		ss->objects[id] = this;
	}

	virtual ~base_object(void) {
		debug(std::cout << "called ~base_object on " << this << "(id=" << id << ")" << std::endl);
		if (refcount == 0) {
			if (bsid) {
				ss->backstore.deallocate(bsid);
				debug(std::cout << "~base_object derefs: ");
				debug(dump_refmap(ondisk_referents));
				for (const auto & p : ondisk_referents)
					p.first->unref(p.second);
			}
			ss->cache_manager.note_death(*this);
		}
		if (id)
			ss->objects.erase(id);
	}
	
	// Loads the target if necessary
	void * get_target(bool dirty) {
		assert(pincount);
		if (!target) {
			refcount_map<CacheManager> refcounts;
			std::iostream * in = ss->backstore.get(bsid);
			serialization_context<CacheManager> ctxt(ss, &refcounts);
			{
				iarchive_t ar(*in);
				ar.template get_helper<serialization_context<CacheManager> >(&ar) = ctxt;
				read_target(ar);
			}
			ss->backstore.put(in);
			ss->cache_manager.note_load(*this);
			assert(refcounts == ondisk_referents);
		}
		if (dirty)
			ss->cache_manager.note_write(*this);
		else
			ss->cache_manager.note_read(*this);
		isdirty = isdirty || dirty;
		return target;
	}

	void ref(uint64_t amount = 1) {
		refcount += amount;
	}
	
	void unref(uint64_t amount = 1) {
		assert(amount <= refcount);
		refcount -= amount;
		if (refcount == 0 && pincount == 0)
			delete this;
	}
	
	void pin(void) {
		pincount++;
	}

	void unpin(void) {
		pincount--;
		if (refcount == 0 && pincount == 0)
			delete this;
		// Technically, we should consider evicting things at this point,
		// but it's kind of expensive, so we skip it.
	}

	void write_object(std::iostream & strm, refcount_map<CacheManager> & refcounts) {
		serialization_context<CacheManager> header_ctxt(ss, &refcounts);
		oarchive_t header_archive(strm);
		header_archive.template
			get_helper<serialization_context<CacheManager> >(&header_archive) = header_ctxt;
		debug(std::cout << "calling write_target" << std::endl);
		write_target(header_archive);
	}

	void clean(void) override {
		assert(isdirty);

		std::stringstream sstream;
		refcount_map<CacheManager> refcounts;
		write_object(sstream, refcounts);		
		std::string buffer = sstream.str();

		uint64_t newbsid = ss->backstore.allocate(buffer.length());
		std::iostream *out = ss->backstore.get(newbsid);
		out->write(buffer.data(), buffer.length());
		ss->backstore.put(out);

		debug(std::cout << "clean reffing: ");
		debug(dump_refmap(refcounts));
		for (const auto & p : refcounts) {
      p.first->ref(p.second);
		}
		if (bsid > 0) {
			debug(std::cout << "clean unreffing: ");
			debug(dump_refmap(ondisk_referents));
      for (const auto & p : ondisk_referents) {
        p.first->unref(p.second);
			}
			ss->backstore.deallocate(bsid);
		}		
		bsid = newbsid;
		ondisk_referents = refcounts;
		isdirty = false;
		ss->cache_manager.note_clean(*this);
  }
	
	reference_to_cacheable_object<CacheManager> & get_write_unit_ref(void) override {
		return *this;
	}

	typename CacheManager::access_info & get_access_info(void) override {
		return accessinfo;
	}

	bool is_dirty(void) const override {
		return isdirty;
	}

	bool is_pinned(void) const override {
		return pincount > 0;
	}

	bool is_in_memory(void) const {
		return target != NULL;
	}

	uint64_t get_id(void) const {
		return id;
	}
	
	virtual void write_target(oarchive_t & ar) const = 0;
	virtual void read_target(iarchive_t & ar) = 0;

	void base_evict(void) {
		ss->cache_manager.note_evict(*this);
	}

	template<class Archive>
	void save(Archive &ar, const unsigned int version) const {
		ar & id;
		ar & bsid;
		ar & ondisk_referents.size();
		for (const auto & p : ondisk_referents) {
			ar & p.first->id;
			ar & p.second;
		}
		ar & refcount;
	}

	template<class Archive>
	void load(Archive &ar, const unsigned int version) {
		checkpoint_context<CacheManager> &cc 
		= ar.template get_helper<checkpoint_context<CacheManager> >(&ar);
		ar & id;
		ar & bsid;
		uint64_t nreferents;
		ar & nreferents;
		for (uint64_t i = 0; i < nreferents; i++) {
			uint64_t rid, rcnt;
			ar & rid;
			ar & rcnt;
			cc.unresolved_links[rid][this] = rcnt;
		}
		ar & refcount;
	}

	BOOST_SERIALIZATION_SPLIT_MEMBER()

	virtual void register_type(oarchive_t &ar) const = 0;
	
protected:
	uint64_t                   id;
	void                     * target;
	swap_space<CacheManager> * ss;
	uint64_t                   bsid;
	refcount_map<CacheManager> ondisk_referents; 
	uint64_t                   refcount;
	uint64_t                   pincount;
	bool                       isdirty;
	typename CacheManager::access_info accessinfo;
};

template <class Referent, class CacheManager>
class object : public base_object<CacheManager> {
public:
	object(swap_space<CacheManager> *sspace,
				 void *tgt)
		: base_object<CacheManager>(sspace, tgt)
	{}

	~object(void) {
		if (base_object<CacheManager>::target)
			delete (Referent *)base_object<CacheManager>::target;
	}

	virtual void evict(void) override {
		assert(base_object<CacheManager>::target);
		delete (Referent *)base_object<CacheManager>::target;
		base_object<CacheManager>::target = NULL;
		base_object<CacheManager>::base_evict();
	}

	virtual void write_target(oarchive_t & ar) const override {
		ar & *(Referent *)base_object<CacheManager>::target;
	}

	virtual void read_target(iarchive_t & ar) override {
		assert(base_object<CacheManager>::target == NULL);
		base_object<CacheManager>::target = new Referent();
		ar & *(Referent *)base_object<CacheManager>::target;
	}

	template<class Archive>
	void serialize(Archive &ar, const unsigned int version) {
		ar & boost::serialization::base_object<base_object<CacheManager> >(*this);
	}

	void register_type(oarchive_t &ar) const {
		ar.template register_type<object<Referent, CacheManager> >();
	}
};

template <class CacheManager>
class swap_space {
	friend base_object<CacheManager>;
public:
  swap_space(backing_store &bs, CacheManager &cm)
		: backstore(bs),
			cache_manager(cm),
			objects()
	{
		uint64_t rootid = backstore.get_root();
		if (rootid) {
			std::iostream *rootsream = backstore.get(rootid);
			checkpoint_context<CacheManager> cc(this);
			{
				iarchive_t ar(*rootsream);
				ar.template get_helper<checkpoint_context<CacheManager> >(&ar) = cc;
				ar & *this;
			}
			for (const auto & p : cc.unresolved_links) {
				base_object<CacheManager> *obj = objects[p.first];
				for (const auto & q : p.second)
					q.first->ondisk_referents[obj] = q.second;
			}
		}
	}

	~swap_space(void) {
		checkpoint();
	}
	
	void checkpoint(void) {
		cache_manager.checkpoint();

		for (const auto & p : objects)
			p.second->ref();
		for (const auto & p : last_checkpoint)
			p.second->unref();
		
		std::stringstream sstream;
		checkpoint_context<CacheManager> cc(this);
		{
			oarchive_t ar(sstream);
			ar.template get_helper<checkpoint_context<CacheManager> >(&ar) = cc;
			for (const auto & p : objects)
				p.second->register_type(ar);
			ar & *this;
		}		
		std::string buffer = sstream.str();
		uint64_t newbsid = backstore.allocate(buffer.length());
		std::iostream *out = backstore.get(newbsid);
		out->write(buffer.data(), buffer.length());
		backstore.put(out);
		uint64_t oldbsid = backstore.get_root();
		backstore.set_root(newbsid);
		if (oldbsid)
			backstore.deallocate(oldbsid);
		last_checkpoint = objects;
	}

	template<class Referent>
	class pin {
	public:
		pin(void)
			: obj(NULL)
		{}
	
		pin(object<Referent, CacheManager> *o)
			: obj(o)
		{
			obj->pin();
		}

		~pin(void) {
			if (obj)
				obj->unpin();
		}

		const Referent * operator->(void) const {
			return (const Referent *)obj->get_target(false);
		}

		Referent * operator->(void) {
			return (Referent *)obj->get_target(true);
		}

		pin &operator=(const pin &other) {
			if (obj)
				obj->unpin();
			obj = other.obj;
			if (obj)
				obj->pin();
		}

		bool operator==(const pin &other) const {
			return obj == other.obj;
		}
	
		bool operator!=(const pin &other) const {
			return !operator==(other);
		}

		bool is_in_memory(void) const {
			return obj->is_in_memory();
		}

		bool is_dirty(void) const {
			return obj->is_dirty();
		}

	private:
		object<Referent, CacheManager> *obj;
	};

	template <class Referent>
	class pointer {
		friend swap_space;
	public:
		pointer(void)
			: obj(NULL)
		{}
		
		pointer(const pointer &other) {
			if (other.obj)
				other.obj->ref();
			if (obj)
				obj->unref();
			obj = other.obj;
		}

		~pointer(void) {
			if (obj)
				obj->unref();
		}

		pointer & operator=(const pointer &other) {
			if (other.obj)
				other.obj->ref();
			if (obj)
				obj->unref();
			obj = other.obj;
			return *this;
		}

		bool operator==(const pointer &other) const {
			return obj == other.obj;
		}

		// Allow for checking whether ptr == NULL
		bool operator==(const void *other) const {
			return other == NULL && obj == NULL;
		}

		bool operator!=(const pointer &other) const {
			return !operator==(other);
		}

		bool operator!=(const void *other) const {
			return !operator==(other);
		}

		const pin<Referent> operator->(void) const {
			assert(obj);
			return pin<Referent>(obj);
		}

		pin<Referent> operator->(void) {
			assert(obj);
			return pin<Referent>(obj);
		}

		pin<Referent> get_pin(void) {
			assert(obj);
			return pin<Referent>(obj);
		}

		const pin<Referent> get_pin(void) const {
			assert(obj);
			return pin<Referent>(obj);
		}
	
		bool is_in_memory(void) const {
			return obj->is_in_memory();
		}

		bool is_dirty(void) const {
			return obj->is_dirty();
		}

		bool is_pinned(void) const {
			return obj->is_pinned();
		}

		template <class Archive>
		void save(Archive & ar, const unsigned int version) const {
			serialization_context<CacheManager> & context
				= ar.template get_helper<serialization_context<CacheManager> >((void *)&ar);
			if (obj) {
				ar & obj->get_id();
				(*context.refcounts)[obj]++;
			} else {
				ar & 0ULL;
			}
		}

		template <class Archive>
		void load(Archive &ar, const unsigned int version) {
			serialization_context<CacheManager> & context
				= ar.template get_helper<serialization_context<CacheManager> >((void *)&ar);
			uint64_t target;
			ar & target;
			object<Referent, CacheManager> *newobj = NULL;
			if (target) {
				assert(context.ss->objects.count(target) > 0);
				newobj = dynamic_cast<object<Referent, CacheManager> *>(context.ss->objects[target]);
				(*context.refcounts)[newobj]++;
			}
			if (newobj)
			  newobj->ref();
			if (obj)
			  obj->unref();
			obj = newobj;
		}

		BOOST_SERIALIZATION_SPLIT_MEMBER()

		private:
		object<Referent, CacheManager>     *obj = NULL;
		
		// Only callable through swap_space::allocate(...)
		pointer(object<Referent, CacheManager> *newobj)
			: obj(newobj)
		{
			if (obj)
				obj->ref();
		}
	};

	// template<class Referent, typename... Args>
	// pointer<Referent> allocate(Args... args) {
	// 	Referent *target = new Referent(args...);
	// 	object<Referent, CacheManager> * newobj = new object<Referent, CacheManager>(this, target);
  //   return pointer<Referent>(newobj);
  // }
	
	template <class Referent>
	pointer<Referent> allocate(void) {
		Referent *target = new Referent();
		object<Referent, CacheManager> * newobj = new object<Referent, CacheManager>(this, target);
		pointer<Referent> p(newobj);
		cache_manager.note_birth(*newobj);
    return p;
  }

	template <class Referent>
	void set_root(pointer<Referent> p) {
		assert(p.obj);
		base_object<CacheManager> *old_root = root;
		root = p.obj;
		if (root)
			root->ref();
		if (old_root)
			old_root->unref();
	}

	template <class Referent>
	pointer<Referent> get_root(void) {
		return pointer<Referent>(root);
	}
	
	
	template<class Archive>
	void save(Archive &ar, const unsigned int version) const {
		ar & next_id;
		ar & objects;
		if (root)
			ar & root->get_id();
		else
			ar & 0ULL;
	}

	template<class Archive>
	void load(Archive &ar, const unsigned int version) {
		ar & next_id;
		ar & objects;
		uint64_t rootid;
		ar & rootid;
		if(rootid)
			root = objects[rootid];
	}

	BOOST_SERIALIZATION_SPLIT_MEMBER()
	
	protected:
	backing_store &backstore;
	CacheManager &cache_manager;
  std::unordered_map<uint64_t, base_object<CacheManager> *> last_checkpoint;
  std::unordered_map<uint64_t, base_object<CacheManager> *> objects;
	base_object<CacheManager> *root = NULL;
  uint64_t next_id = 1;
};




#endif // SWAP_SPACE_HPP
