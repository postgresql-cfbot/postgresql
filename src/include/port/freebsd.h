/* src/include/port/freebsd.h */

/*
 * FreeBSD 13 gained O_DSYNC support.  Set the default wal_sync_method to
 * fdatasync, because xlogdefs.h's normal rules would prefer open_datasync.
 * That wouldn't be a good default, because 8KB direct writes interact badly
 * with the default 32KB block size on UFS, requiring read-before-write.
 */
#ifdef HAVE_FDATASYNC
#define PLATFORM_DEFAULT_SYNC_METHOD	SYNC_METHOD_FDATASYNC
#endif
