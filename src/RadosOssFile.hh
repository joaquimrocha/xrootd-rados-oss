/************************************************************************
 * Rados OSS Plugin for XRootD                                          *
 * Copyright © 2013 CERN/Switzerland                                    *
 *                                                                      *
 * Author: Joaquim Rocha <joaquim.rocha@cern.ch>                        *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __RADOS_OSS_FILE_HH__
#define __RADOS_OSS_FILE_HH__

#include <xrootd/XrdOss/XrdOss.hh>
#include <vector>
#include <radosfs/Filesystem.hh>
#include <radosfs/File.hh>

#include "RadosOss.hh"

class RadosOssFile : public XrdOssDF
{
public:
  RadosOssFile(radosfs::Filesystem *radosFs, const XrdSysError &eroute);
  virtual ~RadosOssFile();
  virtual int Open(const char *path, int flags, mode_t mode, XrdOucEnv &env);
  virtual int Close(long long *retsz=0);
  virtual ssize_t Read(off_t offset, size_t blen);
  virtual ssize_t Read(void *buff, off_t offset, size_t blen);
  virtual int Fstat(struct stat *buff);
  virtual ssize_t Write(const void *buff, off_t offset, size_t blen);
  virtual int getFD() { return fd; }

private:
  radosfs::Filesystem *mRadosFs;
  radosfs::File *mFile;
  char* mObjectName;
  XrdSysMutex mMutex;
  XrdSysError mEroute;
  uid_t mUid;
  gid_t mGid;
};

#endif /* __RADOS_OSS_FILE_HH__ */
