// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MDS_EOPEN_H
#define __MDS_EOPEN_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EOpen : public LogEvent {
public:
  EMetaBlob metablob;
  list<inodeno_t> inos;

  EOpen() : LogEvent(EVENT_OPEN) { }
  EOpen(CInode *in) : LogEvent(EVENT_OPEN) {
    add_inode(in);
  }
  void print(ostream& out) {
    out << "EOpen " << metablob;
  }

  void add_inode(CInode *in) {
    inos.push_back(in->ino());
    metablob.add_primary_dentry(in->get_parent_dn(), false);
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(inos, bl);
    metablob._encode(bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(inos, bl, off);
    metablob._decode(bl, off);
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

#endif
