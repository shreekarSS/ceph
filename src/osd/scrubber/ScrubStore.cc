// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ScrubStore.h"
#include "osd/osd_types.h"
#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"
#include "common/debug.h" 

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

#define dout_context g_ceph_context // Use the global Ceph context
#define dout_subsys ceph_subsys_osd // Use the OSD subsystem for debug output

// Define dout_prefix if needed
#define dout_prefix *_dout

namespace {
ghobject_t make_scrub_object(const spg_t& pgid, bool deep = false)
{
  ostringstream ss;
  ss << (deep ? "deep_scrub_" : "scrub_") << pgid;
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
  ghobject_t oid = make_scrub_object(pgid);
  ghobject_t deep_oid = make_scrub_object(pgid, true);
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
  ceph_assert(results.empty());
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
    dout(10) << "Debug message here" << dendl;

  } else {
    results[to_object_key(pool, e.object)] = bl;
    dout(10) << "Debug message here too" << dendl;

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
  return results.empty() and deep_results.empty();
}

void Store::flush(ObjectStore::Transaction* t)
{
  
  if (t) {

    OSDriver::OSTransaction txn = driver.get_transaction(t);
    backend.set_keys(results, &txn);

    OSDriver::OSTransaction deep_txn = deep_driver.get_transaction(t);
    deep_backend.set_key(deep_results, &deep_txn);

  }
  results.clear();
  deep_results.clear();
}

void Store::cleanup(ObjectStore::Transaction* t, bool is_deep_scrub)
{
  t->remove(coll, hoid); ; // Always clear the shallow error database
  if (is_deep_scrub){
      t->remove(coll, deep_hoid);  // For deep scrubs, also clear the deep error database
  }
}

std::vector<bufferlist>
Store::get_snap_errors(int64_t pool,
		       const librados::object_id_t& start,
		       uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist>
Store::get_object_errors(int64_t pool,
			 const librados::object_id_t& start,
			 uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_object_key(pool) : to_object_key(pool, start));
  const string end = last_object_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist>
Store::get_errors(const string& begin,
		  const string& end,
		  uint64_t max_return) const
{
  vector<bufferlist> errors;
  auto next = std::make_pair(begin, bufferlist{});
  while (max_return && !backend.get_next(next.first, &next)) {
    if (next.first >= end)
      break;
    errors.push_back(next.second);
    max_return--;
  }
  return errors;
}

} // namespace Scrub