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
#include <assert.h>
#include <fcntl.h>
#include <XrdSys/XrdSysPlatform.hh>
#include <XrdOuc/XrdOucEnv.hh>

#include "RadosOssDir.hh"
#include "RadosOssDefines.hh"

RadosOssDir::RadosOssDir(RadosOss *cephOss,
                         const XrdSysError &eroute)
  : mCephOss(cephOss),
    mDirInfo(0),
    mNextEntry(0)
{
}

RadosOssDir::~RadosOssDir()
{
}

int
RadosOssDir::Opendir(const char *path, XrdOucEnv &env)
{
  std::string dirPath(path);
  int ret;

  if (dirPath[dirPath.length() - 1] != '/')
    dirPath += '/';

  mDirInfo = mCephOss->getDirInfo(dirPath.c_str());
  mDirInfo->update();

  uid_t uid = env.GetInt("uid");
  gid_t gid = env.GetInt("gid");

  if (mDirInfo->hasPermission(uid, gid, O_RDONLY))
    return XrdOssOK;

  return -EACCES;
}

int
RadosOssDir::Close(long long *retsz)
{
  return XrdOssOK;
}

int
RadosOssDir::Readdir(char *buff, int blen)
{
  const std::string &entry = mDirInfo->getEntry(mNextEntry++);

  if (entry != "")
    strlcpy(buff, entry.c_str(), entry.length());
  else
    *buff = '\0';

  return XrdOssOK;
}
