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

#ifndef __RADOS_OSS_DEFINES_HH__
#define __RADOS_OSS_DEFINES_HH__

#define RADOS_OSS_CONFIG_PREFIX "radososs"
#define RADOS_CONFIG (RADOS_OSS_CONFIG_PREFIX ".config")
#define RADOS_CONFIG_POOLS_KW (RADOS_OSS_CONFIG_PREFIX ".pools")
#define RADOS_CONFIG_USER (RADOS_OSS_CONFIG_PREFIX ".user")
#define BYTE_CONVERSION 1000000 // from MB
#define DEFAULT_POOL_PREFIX "/"
#define DEFAULT_POOL_FILE_SIZE 1000 // 1 GB
#define XATTR_PERMISSIONS_LENGTH 50
#define ROOT_UID 0
#define XATTR_UID "uid="
#define XATTR_GID "gid="
#define XATTR_MODE "mode="
#define XATTR_PERMISSIONS "permissions"
#define DEFAULT_MODE (S_IFREG | S_IRWXU | S_IRGRP | S_IROTH)
#define INDEX_NAME_KEY "name="
#define PATH_SEP '/'

#endif // __RADOS_OSS_DEFINES_HH__
