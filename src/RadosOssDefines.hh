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

#define RADOS_CONFIG (RADOS_OSS_CONFIG_PREFIX ".config")
#define RADOS_CONFIG_USER (RADOS_OSS_CONFIG_PREFIX ".user")
#define RADOS_CONFIG_DEFAULT_STRIPESIZE (RADOS_OSS_CONFIG_PREFIX ".stripe")
#define RADOS_CONFIG_DATA_POOLS (RADOS_OSS_CONFIG_PREFIX ".datapools")
#define RADOS_CONFIG_MTD_POOLS (RADOS_OSS_CONFIG_PREFIX ".metadatapools")
#define RADOS_OSS_CONFIG_PREFIX "radososs"
#define DEFAULT_POOL_PREFIX "/"
#define DEFAULT_POOL_FILE_SIZE 1000 // 1 GB
#define ROOT_UID 0

#endif // __RADOS_OSS_DEFINES_HH__
