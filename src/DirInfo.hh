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

#ifndef __DIR_INFO_HH__
#define __DIR_INFO_HH__

#include <map>
#include <string>
#include <rados/librados.h>

class DirInfo
{
public:
  DirInfo(void);
  DirInfo(const std::string &dirpath, rados_ioctx_t ioctx);
  virtual ~DirInfo(void);

  int update(void);
  const std::string getEntry(int index);
  bool hasPermission(uid_t uid, gid_t gid, mode_t mode);

private:
  void parseContents(char *buff, int length);

  std::string mPath;
  rados_ioctx_t mIoctx;
  std::map<std::string, int> mContentsMap;
  uint64_t mLastCachedSize;
  int mLastReadByte;
  uid_t mUid;
  gid_t mGid;
  mode_t mMode;
};

#endif /* __DIR_INFO_HH__ */
