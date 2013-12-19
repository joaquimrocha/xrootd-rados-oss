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

#ifndef __RADOS_OSS_HH__
#define __RADOS_OSS_HH__

#include <XrdOss/XrdOss.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <stdio.h>
#include <vector>
#include <string>
#include <map>
#include <set>

#include "DirInfo.hh"

typedef struct {
  std::string name;
  int size;
  rados_ioctx_t ioctx;
} RadosOssPool;

class RadosOss : public XrdOss
{
public:
  virtual XrdOssDF *newDir(const char *tident) { return 0; }
  virtual XrdOssDF *newFile(const char *tident);

  virtual int     Chmod(const char *, mode_t mode, XrdOucEnv *eP=0)
                        { return -ENOTSUP; }
  virtual int     Create(const char *, const char *, mode_t, XrdOucEnv &,
                         int opts=0);
  virtual int     Init(XrdSysLogger *, const char *);
  virtual int     Mkdir(const char *, mode_t mode, int mkpath=0,
                        XrdOucEnv *eP=0) { return -ENOTSUP; }
  virtual int     Remdir(const char *, int Opts=0, XrdOucEnv *eP=0)
                         { return -ENOTSUP; }
  virtual int     Rename(const char *, const char *,
                         XrdOucEnv *eP1=0, XrdOucEnv *eP2=0)
                         { return -ENOTSUP; }
  virtual int     Stat(const char *, struct stat *, int opts=0, XrdOucEnv *eP=0);
  virtual int     StatFS(const char *path, char *buff, int &blen, XrdOucEnv *eP=0);
  virtual int     Truncate(const char *, unsigned long long, XrdOucEnv *eP=0);
  virtual int     Unlink(const char *path, int Opts=0, XrdOucEnv *eP=0);

  static bool hasPermission(const struct stat &buff,
                            const uid_t uid,
                            const gid_t gid,
                            const int permission);

  static int genericStat(rados_ioctx_t &ioctx,
                         const char* path,
                         struct stat* buff);
  const RadosOssPool * getPoolFromPath(const std::string &path);

  DirInfo *getDirInfo(const char *path);

  RadosOss();
  virtual ~RadosOss();
  XrdSysMutex mutex;

private:
  int loadInfoFromConfig(const char *pluginConf,
                         std::string &configPath,
                         std::string &userName);
  void addPoolFromConfStr(const char *confStr);
  void initIoctxInPools(void);
  std::string getDefaultPoolName(void) const;
  int getIoctxFromPath(const std::string &objectName, rados_ioctx_t *ioctx);

  rados_t mCephCluster;
  std::vector<rados_completion_t> mCompletionList;
  std::map<std::string, RadosOssPool> mPoolMap;
  std::set<std::string> mPoolPrefixSet;
  std::map<std::string, DirInfo> mDirCache;
};

#endif /* __RADOS_OSS_HH__ */
