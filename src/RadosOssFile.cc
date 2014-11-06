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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <private/XrdOss/XrdOssError.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <stdio.h>
#include <string>
#include <radosfs/RadosFsFile.hh>

#include "RadosOssFile.hh"
#include "RadosOssDefines.hh"

RadosOssFile::RadosOssFile(radosfs::RadosFs *radosFs, const XrdSysError &eroute)
  : mRadosFs(radosFs),
    mFile(0),
    mObjectName(0),
    mEroute(eroute)
{
  fd = -1;
}

RadosOssFile::~RadosOssFile()
{
  delete mFile;
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
  int ret = 0;
  mObjectName = strdup(path);
  radosfs::RadosFsFile::OpenMode openMode = radosfs::RadosFsFile::MODE_READ;

  if (flags & O_RDWR)
    openMode = (radosfs::RadosFsFile::OpenMode)
        (radosfs::RadosFsFile::MODE_WRITE | radosfs::RadosFsFile::MODE_READ);
  else if (flags & O_WRONLY)
    openMode = (radosfs::RadosFsFile::OpenMode) radosfs::RadosFsFile::MODE_WRITE;

  mFile = new radosfs::RadosFsFile(mRadosFs, path, openMode);

  if (flags & O_CREAT)
    ret = mFile->create(-1, std::string(""), env.Get("rfs.stripe") ? atoi(env.Get("rfs.stripe")) : 0 );

  if (flags & O_TRUNC)
    ret = mFile->truncate(0);

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
  return mFile->read((char *) buff, offset, blen);
}

int
RadosOssFile::Fstat(struct stat *buff)
{
  return mRadosFs->stat(mObjectName, buff);
}

ssize_t
RadosOssFile::Write(const void *buff, off_t offset, size_t blen)
{
  int ret = mFile->write((char *) buff, offset, blen);

  // The libradosfs file write returns 0 if it succeeds but the XRootD OSS Write
  // needs to return the number of bytes instead
  if (ret == 0)
    ret = blen;

  return ret;
}
