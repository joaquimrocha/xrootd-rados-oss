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

#include <rados/librados.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream>
#include <XrdSys/XrdSysError.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdVersion.hh>

#include "hash64.h"
#include "RadosOss.hh"
#include "RadosOssFile.hh"
#include "RadosOssDefines.hh"

extern XrdSysError OssEroute;

#define LOG_PREFIX "--- Ceph Oss Rados --- "

extern "C"
{
  XrdOss*
  XrdOssGetStorageSystem(XrdOss* native_oss,
                         XrdSysLogger* Logger,
                         const char* config_fn,
                         const char* parms)
  {
    OssEroute.SetPrefix(LOG_PREFIX);
    OssEroute.logger(Logger);
    RadosOss* cephOss = new RadosOss();

    return (cephOss->Init(Logger, config_fn) ? 0 : (XrdOss*) cephOss);
  }
}

RadosOss::RadosOss()
{
  int err = rados_create(&mCephCluster, 0);

  if (err < 0)
    OssEroute.Emsg("Problem when creating the cluster", strerror(-err));
}

RadosOss::~RadosOss()
{
  std::map<std::string, RadosOssPool>::iterator it;
  for (it = mPoolMap.begin(); it != mPoolMap.end(); it++)
  {
    rados_ioctx_destroy((*it).second.ioctx);
  }

  rados_shutdown(mCephCluster);
}

int
RadosOss::Init(XrdSysLogger *logger, const char *configFn)
{
  mConfigFN = configFn;

  int ret = loadInfoFromConfig();

  if (ret != 0)
  {
    OssEroute.Emsg("Problem when reading Ceph's config file", mConfigFN,
                   ":", strerror(-ret));
    return ret;
  }

  ret = rados_connect(mCephCluster);

  if (ret == 0 && mPoolMap.count(DEFAULT_POOL_PREFIX) == 0)
  {
    RadosOssPool defaultPool = {getDefaultPoolName(), DEFAULT_POOL_FILE_SIZE};
    mPoolMap[DEFAULT_POOL_PREFIX] = defaultPool;
    mPoolPrefixSet.insert(DEFAULT_POOL_PREFIX);

    OssEroute.Emsg("Got default pool name since none was configured",
                   DEFAULT_POOL_PREFIX, "=", defaultPool.name.c_str());
  }

  initIoctxInPools();

  return ret;
}

std::string
RadosOss::getDefaultPoolName() const
{
  const int poolListMaxSize = 1024;
  char poolList[poolListMaxSize];
  rados_pool_list(mCephCluster, poolList, poolListMaxSize);

  return poolList;
}

int
RadosOss::loadInfoFromConfig()
{
  XrdOucStream Config;
  int cfgFD;
  char *var, *configPath = 0;

  if ((cfgFD = open(mConfigFN, O_RDONLY, 0)) < 0)
    return cfgFD;

  Config.Attach(cfgFD);
  while ((var = Config.GetMyFirstWord()))
  {
    if (configPath == 0 && strcmp(var, RADOS_CONFIG) == 0)
    {
      configPath = Config.GetWord();
    }

    if (strcmp(var, RADOS_CONFIG_POOLS_KW) == 0)
    {
      const char *pool;
      while (pool = Config.GetWord())
        addPoolFromConfStr(pool);
    }
  }

  Config.Close();

  return rados_conf_read_file(mCephCluster, configPath);
}

void
RadosOss::addPoolFromConfStr(const char *confStr)
{
  int delimeterIndex;
  XrdOucString str(confStr);
  XrdOucString poolSize("");

  delimeterIndex = str.find(':');
  if (delimeterIndex == STR_NPOS || delimeterIndex == 0 ||
      delimeterIndex == str.length() - 1)
  {
    OssEroute.Emsg("Error splitting the pool conf str", confStr);
    return;
  }

  RadosOssPool pool = {"", DEFAULT_POOL_FILE_SIZE};
  XrdOucString poolPrefix(str, 0, delimeterIndex - 1);
  XrdOucString poolName(str, delimeterIndex + 1);

  delimeterIndex = poolName.find(':');
  if (delimeterIndex == STR_NPOS || delimeterIndex == 0 ||
      delimeterIndex == poolName.length() - 1)
  {
    pool.size = DEFAULT_POOL_FILE_SIZE;
  }
  else
  {
    poolSize = XrdOucString(poolName, delimeterIndex + 1);
    poolName.erase(delimeterIndex, poolName.length() - delimeterIndex);
    pool.size = atoi(poolSize.c_str());

    if (pool.size == 0)
      pool.size = DEFAULT_POOL_FILE_SIZE;
  }

  pool.name = poolName.c_str();

  OssEroute.Say(LOG_PREFIX "Found pool with name ", poolName.c_str(),
                " for prefix ", poolPrefix.c_str());

  if (poolSize != "")
    OssEroute.Say(LOG_PREFIX "... and size configured to ", poolSize.c_str(),
                  " MB");

  mPoolMap[poolPrefix.c_str()] = pool;
  // We keep a set to quickly look for the prefix though
  // in the future we could implement a trie for improved efficiency
  mPoolPrefixSet.insert(poolPrefix.c_str());
}

void
RadosOss::initIoctxInPools()
{
  std::map<std::string, RadosOssPool>::iterator it = mPoolMap.begin();

  while (it != mPoolMap.end())
  {
    const std::string &key = (*it).first;
    RadosOssPool &pool = (*it).second;
    int res = rados_ioctx_create(mCephCluster, pool.name.c_str(), &pool.ioctx);

    it++;

    if (res != 0)
    {
      OssEroute.Emsg("Problem creating pool from name",
                     pool.name.c_str(), strerror(-res));
      mPoolMap.erase(key);
    }
  }
}

int
RadosOss::getIoctxFromPath(const std::string &objectName,
                           rados_ioctx_t *ioctx)
{
  const RadosOssPool *pool = getPoolFromPath(objectName);

  if (!pool)
  {
    OssEroute.Emsg("No pool was found for object name", objectName.c_str());
    return -ENODEV;
  }

  *ioctx = pool->ioctx;

  return 0;
}

static ino_t
hash(const char *path)
{
  return hash64((ub1 *) path, strlen(path), 0);
}

int
RadosOss::genericStat(rados_ioctx_t &ioctx,
                      const char* path,
                      struct stat* buff)
{
  uint64_t psize;
  time_t pmtime;
  int ret;
  uid_t uid = 0;
  gid_t gid = 0;
  mode_t permissions = DEFAULT_MODE;
  char permXAttr[XATTR_INT_LENGTH];
  char uidXAttr[XATTR_INT_LENGTH];
  char gidXAttr[XATTR_INT_LENGTH];

  ret = rados_stat(ioctx, path, &psize, &pmtime);

  if (ret != 0)
    return ret;

  if (rados_getxattr(ioctx, path, XATTR_UID, uidXAttr, XATTR_INT_LENGTH) >= 0)
    uid = atoi(uidXAttr);

  if (rados_getxattr(ioctx, path, XATTR_GID, gidXAttr, XATTR_INT_LENGTH) >= 0)
    gid = atoi(gidXAttr);

  if (rados_getxattr(ioctx, path, XATTR_MODE, permXAttr, XATTR_INT_LENGTH) >= 0)
    permissions = (mode_t) strtoul(permXAttr, 0, 8);

  buff->st_dev = 0;
  buff->st_ino = hash(path);
  buff->st_mode = permissions;
  buff->st_nlink = 1;
  buff->st_uid = uid;
  buff->st_gid = gid;
  buff->st_rdev = 0;
  buff->st_size = psize;
  buff->st_blksize = 4;
  buff->st_blocks = buff->st_size / buff->st_blksize;
  buff->st_atime = pmtime;
  buff->st_mtime = pmtime;
  buff->st_ctime = pmtime;

  return ret;
}

bool
RadosOss::hasPermission(const struct stat &buff,
                        const uid_t uid,
                        const gid_t gid,
                        const int permission)
{
  if (uid == ROOT_UID)
    return true;

  mode_t usrPerm = S_IRUSR;
  mode_t grpPerm = S_IRGRP;
  mode_t othPerm = S_IROTH;

  if (permission != O_RDONLY)
  {
    usrPerm = S_IWUSR;
    grpPerm = S_IWGRP;
    othPerm = S_IWOTH;
  }

  if (buff.st_uid == uid && (buff.st_mode & usrPerm))
    return true;
  if (buff.st_gid == gid && (buff.st_mode & grpPerm))
    return true;
  if (buff.st_mode & othPerm)
    return true;

  return false;
}

int
RadosOss::Stat(const char* path,
               struct stat* buff,
               int opts,
               XrdOucEnv* env)
{
  rados_ioctx_t ioctx;
  int ret = getIoctxFromPath(path, &ioctx);

  if (ret != 0)
  {
    OssEroute.Emsg("Failed to get Ioctx", strerror(-ret));
    return ret;
  }

  ret = genericStat(ioctx, path, buff);

  if (ret != 0)
    OssEroute.Emsg("Failed to stat file", strerror(-ret));

  return ret;
}

int
RadosOss::Unlink(const char *path, int Opts, XrdOucEnv *env)
{
  uid_t uid;
  gid_t gid;
  rados_ioctx_t ioctx;
  struct stat buff;
  int ret = getIoctxFromPath(path, &ioctx);

  if (ret != 0)
  {
    OssEroute.Emsg("Failed to get Ioctx", strerror(-ret));
    return ret;
  }

  uid = env->GetInt("uid");
  gid = env->GetInt("gid");

  ret = genericStat(ioctx, path, &buff);

  if (ret != 0)
    OssEroute.Emsg("Failed to stat file", path, ":", strerror(-ret));
  else if (RadosOss::hasPermission(buff, uid, gid, O_WRONLY | O_RDWR))
    ret = rados_remove(ioctx, path);
  else
    OssEroute.Emsg("No permissions to remove", path);

  return ret;
}

int
RadosOss::Truncate(const char* path,
                   unsigned long long size,
                   XrdOucEnv* env)
{
  uid_t uid;
  gid_t gid;
  rados_ioctx_t ioctx;
  struct stat buff;
  int ret = getIoctxFromPath(path, &ioctx);

  if (ret != 0)
  {
    OssEroute.Emsg("Failed to get Ioctx", strerror(-ret));
    return ret;
  }

  uid = env->GetInt("uid");
  gid = env->GetInt("gid");

  ret = genericStat(ioctx, path, &buff);

  if (ret != 0)
    OssEroute.Emsg("Failed to stat file", path, ":", strerror(-ret));
  else if (RadosOss::hasPermission(buff, uid, gid, O_WRONLY | O_RDWR))
    ret = rados_trunc(ioctx, path, size);

  return ret;
}

XrdOssDF *
RadosOss::newFile(const char *tident)
{
  return dynamic_cast<XrdOssDF *>(new RadosOssFile(this, OssEroute));
}

static bool
verifyIsOctal(const char *mode)
{
  const char *ptr = mode;
  while (*ptr != '\0')
  {
    if (*ptr < '0' || *ptr > '7')
      return false;
    ptr++;
  }

  return true;
}

int
RadosOss::Create(const char *tident, const char *path, mode_t access_mode,
                 XrdOucEnv &env, int Opts)
{
  rados_ioctx_t ioctx;
  struct stat buff;
  ostringstream convert;
  int ret = getIoctxFromPath(path, &ioctx);

  if (ret != 0)
  {
    OssEroute.Emsg("Failed to retrieve Ioctx from path when "
                   "attempting to create it", path, ":", strerror(-ret));
    return ret;
  }

  int uid = env.GetInt("uid");
  int gid = env.GetInt("gid");
  const char *permissions = env.Get("mode");
  long int permOctal;

  ret = genericStat(ioctx, path, &buff);

  if (ret == 0 && !hasPermission(buff, uid, gid, O_WRONLY | O_RDWR))
  {
    ret = -1;
    OssEroute.Emsg("Permission denied for file", path);
    goto bailout;
  }

  ret = rados_write(ioctx, path, 0, 0, 0);

  if (ret != 0)
  {
    OssEroute.Emsg("Couldn't write file for creation", path, ":", strerror(-ret));
    goto bailout;
  }

  if (permissions == 0 || strcmp(permissions, "") == 0)
  {
    permOctal = DEFAULT_MODE;
  }
  else if (!verifyIsOctal(permissions))
  {
    OssEroute.Emsg("Unrecognized permissions", permissions,
                   ". Setting default ones...");
    permOctal = DEFAULT_MODE;
  }
  else
  {
    permOctal = strtoul(permissions, 0, 8);
  }

  convert << oct << permOctal;
  ret = rados_setxattr(ioctx, path, XATTR_MODE,
                       convert.str().c_str(), XATTR_INT_LENGTH);
  if (ret != 0)
  {
    OssEroute.Emsg("Error setting 'mode' XAttr for file",
                   path, ":", strerror(-ret));
    goto bailout;
  }

  convert.str("");
  convert.clear();
  convert << dec << uid;

  ret = rados_setxattr(ioctx, path, XATTR_UID,
                       convert.str().c_str(), XATTR_INT_LENGTH);
  if (ret != 0)
  {
    OssEroute.Emsg("Error setting 'uid' XAttr for file",
                   path, ":", strerror(-ret));
    goto bailout;
  }

  convert.str("");
  convert.clear();
  convert << gid;

  ret = rados_setxattr(ioctx, path, XATTR_GID,
                       convert.str().c_str(), XATTR_INT_LENGTH);
  if (ret != 0)
  {
    OssEroute.Emsg("Error setting 'gid' XAttr for file",
                   path, ":", strerror(-ret));
    goto bailout;
  }

 bailout:

  return ret;
}

int
RadosOss::StatFS(const char *path, char *buff, int &blen, XrdOucEnv *eP)
{
  rados_cluster_stat_t stat;
  int valid = rados_cluster_stat(mCephCluster, &stat) == 0;

  blen = snprintf(buff, blen, "%d %lld %d %d %lld %d",
		  valid, (valid ? stat.kb : 0LL), (valid ? stat.kb_used : 0),
		  valid, (valid ? stat.kb : 0LL), (valid ? stat.kb_used : 0));

  return XrdOssOK;
}

const RadosOssPool *
RadosOss::getPoolFromPath(const std::string &path)
{
  std:set<std::string>::reverse_iterator it;
  for (it = mPoolPrefixSet.rbegin(); it != mPoolPrefixSet.rend(); it++)
  {
    if (path.compare(0, (*it).length(), *it) == 0)
      return &mPoolMap[*it];
  }

  return 0;
}

XrdVERSIONINFO(XrdOssGetStorageSystem, RadosOss);
