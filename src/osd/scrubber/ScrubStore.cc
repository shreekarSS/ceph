// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ScrubStore.h"
#include "osd/osd_types.h"
#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"
#include "common/debug.h" 
#include "./pg_scrubber.h"

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

#define dout_context g_ceph_context // Use the global Ceph context
#define dout_subsys ceph_subsys_osd // Use the OSD subsystem for debug output

// Define dout_prefix if needed
#define dout_prefix *_dout

namespace {
ghobject_t make_scrub_object(const spg_t& pgid, scrub_level_t level)
{
  ostringstream ss;
  ss << ((level == scrub_level_t::deep) ? "deep_scrub_" : "scrub_") << pgid;
  return pgid.make_temp_ghobject(ss.str());
}

string first_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0,		// hash
			pool,
			oid.nspace);
  hoid.build_hash_cache();

  return "SCRUB_OBJ_" + hoid.to_str();
}

string last_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // the representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0x77777777, // hash
			pool,
			oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string last_snap_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}
}

namespace Scrub {

Store*
Store::create(ObjectStore* store,
	      ObjectStore::Transaction* t,
	      const spg_t& pgid,
	      const coll_t& coll)
{
  ceph_assert(store);
  ceph_assert(t);
  ghobject_t oid = make_scrub_object(pgid, scrub_level_t::shallow);
  ghobject_t deep_oid = make_scrub_object(pgid, scrub_level_t::deep);
  t->touch(coll, oid);
  t->touch(coll, deep_oid);

  return new Store{coll, oid, deep_oid, store};
}

Store::Store(const coll_t& coll, const ghobject_t& oid, const ghobject_t& deep_oid, ObjectStore* store)
  : coll(coll),
    hoid(oid),
    deep_hoid(deep_oid),
    driver(store, coll, hoid),
    deep_driver(store, coll, deep_oid),
    backend(&driver),
    deep_backend(&deep_driver)
{}

Store::~Store()
{
  ceph_assert(results.empty() && deep_results.empty());
}

void Store::add_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  add_object_error(pool, e);
}

void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  if (e.has_deep_errors()) {
    deep_results[to_object_key(pool, e.object)] = bl;
  }
  else{
    // storing shallow errors separately in results cache
    results[to_object_key(pool, e.object)] = bl;
}
}

void Store::add_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  add_snap_error(pool, e);
}

void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  results[to_snap_key(pool, e.object)] = bl;
}

bool Store::empty() const
{
  return results.empty() && deep_results.empty();
}

void Store::flush(ObjectStore::Transaction* t)
{
  if (t) {
    OSDriver::OSTransaction txn = driver.get_transaction(t);
    backend.set_keys(results, &txn);
    
    OSDriver::OSTransaction deep_txn = deep_driver.get_transaction(t);
    deep_backend.set_keys(deep_results, &deep_txn);
    }
  }
    results.clear();
    deep_results.clear();
}

void Store::cleanup(ObjectStore::Transaction* t, scrub_level_t level)
{
    if (level == scrub_level_t::shallow) {
        t->remove(coll, hoid);  // For shallow scrub, clear shallow error database only
    }
  
    if (level == scrub_level_t::deep) {
        t->remove(coll, hoid);
        t->remove(coll, deep_hoid);  // For deep scrubs, also clear both shallow and deep error database
    }
}

std::vector<bufferlist>
Store::get_snap_errors(int64_t pool,
		       const librados::object_id_t& start,
		       uint64_t max_return,bool isScrubActive, bool isDeep) const
{
  const string begin = (start.name.empty() ?
			first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);
  return get_errors(begin, end, max_return, isScrubActive, isDeep);
}

std::vector<bufferlist>
Store::get_object_errors(int64_t pool,
			 const librados::object_id_t& start,
			 uint64_t max_return,bool isScrubActive, bool isDeep) const
{
  const string begin = (start.name.empty() ?
			first_object_key(pool) : to_object_key(pool, start));
  const string end = last_object_key(pool);
  return get_errors(begin, end, max_return, isScrubActive, isDeep);
}

std::vector<bufferlist>
Store::get_errors(const string& begin,
                  const string& end,
                  uint64_t max_return,
                  bool isScrubActive,
                  bool isDeep) const
{
  vector<bufferlist> errors;

  if (!isScrubActive) {
    auto next_shallow = std::make_pair(begin, bufferlist{});
    auto next_deep = std::make_pair(begin, bufferlist{});

    while (max_return) {
      bool got_shallow_error = !backend.get_next(next_shallow.first, &next_shallow);
      bool got_deep_error = !deep_backend.get_next(next_deep.first, &next_deep);

      if (got_shallow_error && next_shallow.first < end) {
        errors.push_back(next_shallow.second);
        max_return--;
      }

      if (got_deep_error && next_deep.first < end) {
        errors.push_back(next_deep.second);
        max_return--;
      }

      // Break if no more errors in both shallow and deep backends or we have reached the end
      if ((!got_shallow_error && !got_deep_error) || (next_shallow.first >= end && next_deep.first >= end)) {
        break;
      }
    }
  } else if (isScrubActive && !isDeep) {
    // Shallow scrub active, retrieve errors from shallow cache and deep DB
    for (const auto& [key, value] : results) {
      if (key >= begin && key < end && errors.size() < max_return) {
        errors.push_back(value);
      }
    }

    auto next_deep = std::make_pair(begin, bufferlist{});
    while (max_return) {
      bool got_deep_error = !deep_backend.get_next(next_deep.first, &next_deep);
      if (got_deep_error && next_deep.first < end) {
        errors.push_back(next_deep.second);
        max_return--;
      }
      if (!got_deep_error || next_deep.first >= end) {
        break;
      }
    }
  } else if (isScrubActive && isDeep) {
    if (results.empty() && deep_results.empty()) {
    dout(10) << "Both results and deep_results are empty" << dendl;
    return;
  }

    // Deep scrub active, retrieve errors from both shallow and deep cache
    for (const auto& [key, value] : results) {
      if (errors.size() < max_return) {
        errors.push_back(value);
      }
    }
    for (const auto& [key, value] : deep_results) {
      if (errors.size() < max_return) {
        errors.push_back(value);
      }
    }

  
}
return errors;
}

// namespace Scrub
