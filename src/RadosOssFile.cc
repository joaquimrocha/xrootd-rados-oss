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

#include <cephfs/libcephfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <private/XrdOss/XrdOssError.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <stdio.h>
#include <string>

#include "RadosOssFile.hh"
#include "RadosOssDefines.hh"

RadosOssFile::RadosOssFile(RadosOss *cephOss, const XrdSysError &eroute)
  : mCephOss(cephOss),
    mObjectName(0),
    mEroute(eroute)
{
  fd = -1;
}

RadosOssFile::~RadosOssFile()
{
  free(mObjectName);
  mObjectName = 0;
}

int
RadosOssFile::Close(long long *retsz)
{
  Fsync();

  return XrdOssOK;
}

int
RadosOssFile::Open(const char *path, int flags, mode_t mode, XrdOucEnv &env)
{
  int ret;
  struct stat buff;
  const RadosOssPool *pool;

  mObjectName = strdup(path);
  pool = mCephOss->getPoolFromPath(mObjectName);

  if (!pool)
  {
    mEroute.Emsg("No pool was found for object name", mObjectName);
    return -ENODEV;
  }

  // we convert because it will be compared in a different unit
  mPoolFileSize = pool->size * BYTE_CONVERSION;

  mIoctx = pool->ioctx;
  mUid = env.GetInt("uid");
  mGid = env.GetInt("gid");

  ret = mCephOss->genericStat(mIoctx, path, &buff);

  // file doesn't exist
  if (ret != 0)
  {
    mEroute.Emsg("Failed to stat file", path, ":", strerror(-ret));
    return ret;
  }

  // if file exists then it cannot open it with the creation flag
  if (flags & O_CREAT)
  {
    mEroute.Emsg("File exists, cannot create it", path);
    return -EEXIST;
  }

  if (!RadosOss::hasPermission(buff, mUid, mGid, flags))
  {
    mEroute.Emsg("No permissions to open file", path);
    return -EACCES;
  }

  if (flags & O_TRUNC)
  {
    ret = rados_trunc(mIoctx, path, 0);

    if (ret != 0)
      mEroute.Emsg("Error truncating file", strerror(-ret));
  }

  return ret;
}

ssize_t
RadosOssFile::Read(off_t offset, size_t blen)
{
  if (fd < 0)
    return (ssize_t)-XRDOSS_E8004;

  return 0;
}

ssize_t
RadosOssFile::Read(void *buff, off_t offset, size_t blen)
{
  return rados_read(mIoctx, mObjectName, (char *) buff, blen, offset);
}

int
RadosOssFile::Fstat(struct stat *buff)
{
  return mCephOss->genericStat(mIoctx, mObjectName, buff);
}

ssize_t
RadosOssFile::Write(const void *buff, off_t offset, size_t blen)
{
  int ret;
  size_t compIndex, readBytes;
  readBytes = blen;

  if (((size_t) offset + blen) > mPoolFileSize)
  {
    mEroute.Emsg("Cannot write file", mObjectName,
                 ". File size exceeds pool's allowed size.");
    return -EFBIG;
  }

  {
    XrdSysMutexHelper mutexHelper(mMutex);
    rados_completion_t comp;
    mCompletionList.push_back(comp);
    compIndex = mCompletionList.size() - 1;

    rados_aio_create_completion(0, 0, 0, &mCompletionList[compIndex]);
    ret = rados_aio_write(mIoctx, mObjectName,
                          mCompletionList[compIndex], (const char *) buff,
                          blen, offset);

    // remove the completion object if something failed
    if (ret != 0)
    {
      mEroute.Emsg("Failed to write async IO", strerror(-ret));

      std::vector<rados_completion_t>::iterator it = mCompletionList.begin();
      std::advance(it, compIndex);
      mCompletionList.erase(it);
      readBytes = 0;
    }
  }

  return readBytes;
}

int
RadosOssFile::Fsync()
{
  {
    XrdSysMutexHelper mutexHelper(mMutex);

    std::vector<rados_completion_t>::iterator it;
    it = mCompletionList.begin();
    while (it != mCompletionList.end())
    {
      int ret = rados_aio_wait_for_complete(*it);

      if (ret < 0)
        mEroute.Emsg("Failed to wait for completion of async IO",
                     strerror(-ret));

      rados_aio_release(*it);
      it = mCompletionList.erase(it);
    }
  }
  return XrdOssOK;
}
