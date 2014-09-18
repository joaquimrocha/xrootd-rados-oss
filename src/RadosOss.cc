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

#include "RadosOss.hh"
#include "RadosOssFile.hh"
#include "RadosOssDir.hh"
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
}

RadosOss::~RadosOss()
{
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

  mRadosFs.init(userName, configPath);

  std::map<std::string, RadosOssPool>::iterator it = mPoolMap.begin();

  while (it != mPoolMap.end())
  {
    const std::string &key = (*it).first;
    RadosOssPool &pool = (*it).second;
    mRadosFs.addPool(pool.name, key, pool.size);

    it++;
  }

  return ret;
}

std::string
RadosOss::getDefaultPoolName() const
{
  std::vector<std::string> pools(mRadosFs.allPoolsInCluster());

  if (pools.size())
    return pools[0];

  return "";
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
}

void
RadosOss::setIdsFromEnv(XrdOucEnv *env)
{
  if (env)
  {
    uid_t uid = env->GetInt("uid");
    gid_t gid = env->GetInt("gid");

    mRadosFs.setIds(uid, gid);
  }
}

int
RadosOss::Stat(const char* path,
               struct stat* buff,
               int opts,
               XrdOucEnv* env)
{
  setIdsFromEnv(env);

  return mRadosFs.stat(path, buff);
}

int
RadosOss::Mkdir(const char *path, mode_t mode, int mkpath, XrdOucEnv *env)
{
  int ret;
  uid_t uid = 0;
  gid_t gid = 0;
  int owner = uid;
  int group = gid;

  if (env)
  {
    uid = owner = env->GetInt("uid");
    gid = group = env->GetInt("gid");

    mRadosFs.setIds(uid, gid);

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

  radosfs::RadosFsDir dir(&mRadosFs, path);
  ret = dir.create(mode, mkpath, owner, group);

  if (ret != 0)
  {
    OssEroute.Emsg("Couldn't create directory", path, ":", strerror(-ret));
    return ret;
  }

  return XrdOssOK;
}

int
RadosOss::Remdir(const char *path, int Opts, XrdOucEnv *env)
{
  int ret;

  setIdsFromEnv(env);

  radosfs::RadosFsDir dir(&mRadosFs, path);
  ret = dir.remove();

  if (ret != 0)
    OssEroute.Emsg("Problem removing directory", strerror(ret));

  return ret;
}

int
RadosOss::Unlink(const char *path, int Opts, XrdOucEnv *env)
{
  int ret;

  setIdsFromEnv(env);

  radosfs::RadosFsFile file(&mRadosFs, path, radosfs::RadosFsFile::MODE_WRITE);
  ret = file.remove();

  if (ret != 0)
    OssEroute.Emsg("Failed to remove file %s: %s", path, strerror(-ret));

  return ret;
}

int
RadosOss::Truncate(const char* path,
                   unsigned long long size,
                   XrdOucEnv* env)
{
  int ret;

  setIdsFromEnv(env);

  radosfs::RadosFsFile file(&mRadosFs, path, radosfs::RadosFsFile::MODE_WRITE);
  ret = file.truncate(size);

  if (ret != 0)
    OssEroute.Emsg("Failed to truncate file %s: %s", path, strerror(-ret));

  return ret;
}

XrdOssDF *
RadosOss::newFile(const char *tident)
{
  return dynamic_cast<XrdOssDF *>(new RadosOssFile(&mRadosFs, OssEroute));
}

XrdOssDF *
RadosOss::newDir(const char *tident)
{
  return dynamic_cast<XrdOssDF *>(new RadosOssDir(&mRadosFs, OssEroute));
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
  int ret;

  setIdsFromEnv(&env);

  radosfs::RadosFsFile file(&mRadosFs, path, radosfs::RadosFsFile::MODE_WRITE);
  ret = file.create(access_mode);

  if (ret != 0)
    OssEroute.Emsg("Failed to truncate file %s: %s", path, strerror(-ret));

  return ret;
}

int
RadosOss::StatFS(const char *path, char *buff, int &blen, XrdOucEnv *eP)
{
  uint64_t total, used;
  int valid = mRadosFs.statCluster(&total, &used, 0, 0);

  blen = snprintf(buff, blen, "%d %llu %llu %d %llu %llu",
                  valid, (valid ? total : 0LL), (valid ? used : 0LL),
                  valid, (valid ? total : 0LL), (valid ? used : 0LL));

  return XrdOssOK;
}


XrdVERSIONINFO(XrdOssGetStorageSystem, RadosOss);
