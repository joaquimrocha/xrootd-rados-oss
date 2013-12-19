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
#include "RadosOssDir.hh"
#include "RadosOssDefines.hh"

extern XrdSysError OssEroute;

#define LOG_PREFIX "--- Ceph Oss Rados --- "

static const std::string getParentDir(const std::string &obj, int *pos);
static int indexObject(rados_ioctx_t &ioctx,
                       const std::string &obj,
                       char op);

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
  std::string userName, configPath;
  int ret = loadInfoFromConfig(configFn, configPath, userName);

  if (ret != 0)
  {
    OssEroute.Emsg("Problem when reading Rados OSS plugin's config file",
                   configFn, ":", strerror(-ret));
    return ret;
  }

  ret = rados_create(&mCephCluster, userName.c_str());

  if (ret != 0)
  {
    OssEroute.Emsg("Problem when creating the cluster", strerror(-ret));
    return ret;
  }

  ret = rados_conf_read_file(mCephCluster, configPath.c_str());

  if (ret != 0)
  {
    OssEroute.Emsg("Problem when reading Ceph's config file",
                   configPath.c_str(), ":", strerror(-ret));
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
RadosOss::loadInfoFromConfig(const char *pluginConf,
                             std::string &configPath,
                             std::string &userName)
{
  XrdOucStream Config;
  int cfgFD;
  char *var;

  configPath = "";
  userName = "";

  if ((cfgFD = open(pluginConf, O_RDONLY, 0)) < 0)
    return cfgFD;

  Config.Attach(cfgFD);
  while ((var = Config.GetMyFirstWord()))
  {
    if (configPath == "" && strcmp(var, RADOS_CONFIG) == 0)
    {
      configPath = Config.GetWord();
    }
    else if (userName == "" && strcmp(var, RADOS_CONFIG_USER) == 0)
    {
      userName = Config.GetWord();
    }
    else if (strcmp(var, RADOS_CONFIG_POOLS_KW) == 0)
    {
      const char *pool;
      while (pool = Config.GetWord())
        addPoolFromConfStr(pool);
    }
  }

  Config.Close();

  return 0;
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

static int
setPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    long int mode,
                    uid_t uid,
                    gid_t gid)
{
  ostringstream convert;

  convert << XATTR_MODE;
  convert << oct << mode;

  convert << " " << XATTR_UID;
  convert << dec << uid;

  convert << " " << XATTR_GID;
  convert << dec << gid;

  return rados_setxattr(ioctx, obj, XATTR_PERMISSIONS,
                        convert.str().c_str(), XATTR_PERMISSIONS_LENGTH);
}

static int
getPermissionsXAttr(rados_ioctx_t &ioctx,
                    const char *obj,
                    mode_t *mode,
                    uid_t *uid,
                    gid_t *gid)
{
  char permXAttr[XATTR_PERMISSIONS_LENGTH];

  int ret = rados_getxattr(ioctx, obj, XATTR_PERMISSIONS,
                           permXAttr, XATTR_PERMISSIONS_LENGTH);
  if (ret < 0)
    return ret;

  XrdOucString token;
  XrdOucString permissions(permXAttr);

  int i = 0;
  while ((i = permissions.tokenize(token, i, ' ')) != -1)
  {
    if (token.beginswith(XATTR_MODE))
    {
      token.erase(0, strlen(XATTR_MODE));
      *mode = (mode_t) strtoul(token.c_str(), 0, 8);
    }
    else if (token.beginswith(XATTR_UID))
    {
      token.erase(0, strlen(XATTR_UID));
      *uid = (uid_t) atoi(token.c_str());
    }
    else if (token.beginswith(XATTR_GID))
    {
      token.erase(0, strlen(XATTR_GID));
      *gid = (gid_t) atoi(token.c_str());
    }
  }

  return 0;
}

static int
checkIfPathExists(rados_ioctx_t &ioctx,
                  const char *path,
                  mode_t *filetype)
{
  const int length = strlen(path);
  bool isDirPath = path[length - 1] == PATH_SEP;

  if (rados_stat(ioctx, path, 0, 0) == 0)
  {
    if (isDirPath)
      *filetype = S_IFDIR;
    else
      *filetype = S_IFREG;
    return 0;
  }

  std::string otherPath(path);

  if (isDirPath)
  {
    // delete the last separator
    otherPath.erase(length - 1, 1);
  }
  else
  {
    otherPath += PATH_SEP;
  }

  if (rados_stat(ioctx, otherPath.c_str(), 0, 0) == 0)
  {
    if (isDirPath)
      *filetype = S_IFREG;
    else
      *filetype = S_IFDIR;

    return 0;
  }

  return -1;
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
  mode_t permissions = DEFAULT_MODE_FILE;
  bool isDir = false;
  std::string realPath(path);

  ret = rados_stat(ioctx, realPath.c_str(), &psize, &pmtime);
  isDir = realPath[realPath.length() - 1] == PATH_SEP;

  if (ret != 0)
  {
    if (isDir)
      return ret;

    realPath += PATH_SEP;

    isDir = rados_stat(ioctx, realPath.c_str(), &psize, &pmtime) == 0;

    if (!isDir)
      return -ENOENT;
  }

  if (isDir)
    permissions = DEFAULT_MODE_DIR;

  ret = getPermissionsXAttr(ioctx, realPath.c_str(), &permissions, &uid, &gid);

  if (ret != 0)
  {
    OssEroute.Emsg("Problem getting permissions of path", realPath.c_str(),
                   strerror(-ret), "Using default permissions in stat.");
    ret = 0;
  }

  buff->st_dev = 0;
  buff->st_ino = hash(realPath.c_str());
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

static std::string
getDirPath(const char *path)
{
  std::string dir(path);

  if (dir[dir.length() - 1] != PATH_SEP)
    dir += PATH_SEP;

  return dir;
}

int
RadosOss::Mkdir(const char *path, mode_t mode, int mkpath, XrdOucEnv *env)
{
  int ret;
  uid_t uid = 0;
  gid_t gid = 0;
  uid_t owner = uid;
  gid_t group = gid;
  mode_t fileType;
  rados_ioctx_t ioctx;
  std::string dir = getDirPath(path);

  ret = getIoctxFromPath(path, &ioctx);

  if (ret != 0)
  {
    OssEroute.Emsg("Failed to get Ioctx", strerror(-ret));
    return ret;
  }

  if (checkIfPathExists(ioctx, path, &fileType) == 0)
    return -EEXIST;

  if (env)
  {
    uid = owner = env->GetInt("uid");
    gid = group = env->GetInt("gid");

    if (uid == ROOT_UID)
    {
      owner = env->GetInt("owner");
      if (owner < 0)
        owner = uid;

      group = env->GetInt("group");
      if (group < 0)
        group = gid;
    }
  }

  mode_t permOctal = mode | S_IFDIR;
  int index;
  const std::string parentDir = getParentDir(dir, &index);

  struct stat buff;
  ret = RadosOss::genericStat(ioctx, parentDir.c_str(), &buff);

  if (ret != 0)
    return ret;

  if (!RadosOss::hasPermission(buff, uid, gid, O_WRONLY | O_RDWR))
    return -EACCES;

  ret = rados_write(ioctx, dir.c_str(), 0, 0, 0);

  if (ret != 0)
  {
    OssEroute.Emsg("Couldn't create directory", dir.c_str(),
                   ":", strerror(-ret));
    return ret;
  }

  ret = setPermissionsXAttr(ioctx, dir.c_str(), permOctal, owner, group);

  if (ret != 0)
    OssEroute.Emsg("Problem setting permissions:", strerror(-ret));

  indexObject(ioctx, dir.c_str(), '+');

  return XrdOssOK;
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
  {
    ret = rados_remove(ioctx, path);
    indexObject(ioctx, path, '-');
  }
  else
  {
    OssEroute.Emsg("No permissions to remove", path);
    ret = -EACCES;
  }

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

XrdOssDF *
RadosOss::newDir(const char *tident)
{
  return dynamic_cast<XrdOssDF *>(new RadosOssDir(this, OssEroute));
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

  if (genericStat(ioctx, path, &buff) == 0)
  {
    if (hasPermission(buff, uid, gid, O_WRONLY | O_RDWR))
    {
      ret = rados_remove(ioctx, path);
      if (ret != 0)
      {
        OssEroute.Emsg("Problem removing file", path);
        goto bailout;
      }
    }
    else
    {
      ret = -EACCES;
      OssEroute.Emsg("Permission denied for file", path);
      goto bailout;
    }
  }
  else
  {
    indexObject(ioctx, path, '+');
  }

  ret = rados_write(ioctx, path, 0, 0, 0);

  if (ret != 0)
  {
    OssEroute.Emsg("Couldn't write file for creation", path, ":", strerror(-ret));
    goto bailout;
  }

  if (permissions == 0 || strcmp(permissions, "") == 0)
  {
    permOctal = DEFAULT_MODE_FILE;
  }
  else if (!verifyIsOctal(permissions))
  {
    OssEroute.Emsg("Unrecognized permissions", permissions,
                   ". Setting default ones...");
    permOctal = DEFAULT_MODE_FILE;
  }
  else
  {
    permOctal = strtoul(permissions, 0, 8);
  }

  ret = setPermissionsXAttr(ioctx, path, permOctal, uid, gid);

  if (ret != 0)
    OssEroute.Emsg("Problem setting permissions:", strerror(-ret));

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

const std::string
getParentDir(const std::string &obj, int *pos)
{
  int length = obj.length();
  int index = obj.rfind(PATH_SEP, length - 2);

  if (length - 1 < 1 || index == STR_NPOS)
    return "";

  index++;

  if (pos)
    *pos = index;

  return obj.substr(0, index);
}

static std::string
escapeObjName(const std::string &obj)
{
  XrdOucString str(obj.c_str());

  str.replace("\"", "\\\"");

  return str.c_str();
}

int
indexObject(rados_ioctx_t &ioctx,
            const std::string &obj,
            char op)
{
  int index;
  std::string contents;
  const std::string &dirName = getParentDir(obj, &index);

  if (dirName == "")
    return 0;

  const std::string &baseName = obj.substr(index, STR_NPOS);

  contents += op;
  contents += INDEX_NAME_KEY "\"" + escapeObjName(baseName) + "\" ";
  contents += "\n";

  return rados_append(ioctx, dirName.c_str(),
                      contents.c_str(), strlen(contents.c_str()));
}

DirInfo *
RadosOss::getDirInfo(const char *path)
{
  DirInfo *info;

  if (mDirCache.count(path) == 0)
  {
    rados_ioctx_t ioctx;
    int ret = getIoctxFromPath(path, &ioctx);

    if (ret != 0)
    {
      OssEroute.Emsg("Failed to get Ioctx", strerror(-ret));
      return 0;
    }

    DirInfo dirInfo(path, ioctx);
    mDirCache.insert(std::pair<std::string, DirInfo>(path, dirInfo));
  }

  return &mDirCache[path];
}

XrdVERSIONINFO(XrdOssGetStorageSystem, RadosOss);
