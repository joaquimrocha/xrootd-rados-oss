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

#ifndef __RADOS_OSS_DIR_HH__
#define __RADOS_OSS_DIR_HH__

#include <xrootd/XrdOss/XrdOss.hh>
#include <xrootd/XrdSys/XrdSysError.hh>
#include <radosfs/Filesystem.hh>
#include <radosfs/Dir.hh>

class RadosOssDir : public XrdOssDF
{
public:
  RadosOssDir(radosfs::Filesystem *radosFs, const XrdSysError &eroute);
  virtual ~RadosOssDir();
  virtual int Opendir(const char *, XrdOucEnv &);
  virtual int Readdir(char *buff, int blen);
  virtual int Close(long long *retsz=0);

private:
  radosfs::Filesystem *mRadosFs;
  radosfs::Dir *mDir;
  std::set<std::string> mEntryList;
  std::set<std::string>::const_iterator mEntryListIt;
};

#endif /* __RADOS_OSS_DIR_HH__ */
