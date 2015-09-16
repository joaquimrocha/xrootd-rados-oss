/************************************************************************
 * Rados OSS Plugin for XRootD                                          *
 * Copyright © 2013-2015 CERN/Switzerland                                    *
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

#include <XrdOuc/XrdOucString.hh>
#include <fcntl.h>
#include "RadosOss.hh"
#include "DirInfo.hh"
#include "RadosOssDefines.hh"

DirInfo::DirInfo(const std::string &dirpath, rados_ioctx_t ioctx)
  : mPath(dirpath),
    mIoctx(ioctx),
    mLastCachedSize(0),
    mLastReadByte(0)
{}

DirInfo::DirInfo()
  : mPath(""),
    mIoctx(0),
    mLastCachedSize(0),
    mLastReadByte(0)
{}

DirInfo::~DirInfo() {};

void
DirInfo::parseContents(char *buff, int length)
{
  XrdOucString line;
  XrdOucString contents(buff);

  int i = 0;
  while ((i = contents.tokenize(line, i, '\n')) != -1)
  {
    bool add = true;
    // we add the name key's length + 2 because we count
    // the operation char (+ or -) and the "
    int namePos = strlen(INDEX_NAME_KEY) + 2;

    if (line.length() < namePos)
      continue;

    // we avoid including the last two chars because they are "\n
    XrdOucString entry(line, namePos, line.length() - 2);
    entry.replace("\\\"", "\"");

    if (mContentsMap.count(entry.c_str()) > 0)
    {
      if (line[0] == '-')
      {
	mContentsMap.erase(entry.c_str());
      }
    }
    else
      mContentsMap[entry.c_str()] = 1;
  }
}

int
DirInfo::update()
{
  struct stat statBuff;
  int ret = RadosOss::genericStat(mIoctx, mPath.c_str(), &statBuff);

  if (ret != 0)
    return ret;

  mUid = statBuff.st_uid;
  mGid = statBuff.st_gid;
  mMode = statBuff.st_mode;

  if (statBuff.st_size == mLastCachedSize)
    return 0;

  uint64_t buffLength = statBuff.st_size - mLastCachedSize;
  char buff[buffLength];

  ret = rados_read(mIoctx, mPath.c_str(), buff, buffLength, mLastReadByte);

  if (ret != 0)
  {
    mLastReadByte = ret;
    buff[buffLength - 1] = '\0';
    parseContents(buff, buffLength);
  }

  mLastCachedSize = mLastReadByte = statBuff.st_size;

  return 0;
}

const std::string
DirInfo::getEntry(int index)
{
  const int size = (int) mContentsMap.size();

  if (index >= size)
    return "";

  std::map<std::string, int>::iterator it = mContentsMap.begin();
  std::advance(it, index);

  return (*it).first;
}

bool
DirInfo::hasPermission(uid_t uid, gid_t gid, mode_t mode)
{
  if (uid == ROOT_UID)
    return true;

  mode_t usrPerm = S_IRUSR;
  mode_t grpPerm = S_IRGRP;
  mode_t othPerm = S_IROTH;

  if ((mode & O_RDONLY) == 0)
  {
    usrPerm = S_IWUSR;
    grpPerm = S_IWGRP;
    othPerm = S_IWOTH;
  }

  if (mUid == uid && (mMode & usrPerm))
    return true;
  if (mGid == gid && (mMode & grpPerm))
    return true;
  if (mMode & othPerm)
    return true;

  return false;
}
