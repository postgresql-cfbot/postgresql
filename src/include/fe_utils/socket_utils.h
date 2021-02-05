/*-------------------------------------------------------------------------
 *
 * Socket-processing utility routines for frontend code
 *
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/fe_utils/socket_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SOCKET_UTILS_H
#define SOCKET_UTILS_H

/*
 * Return true if the file descriptor can be added to the set with the size
 * FD_SETSIZE and false otherwise.
 */
static inline bool
check_fd_set_size(int fd, const fd_set *fds)
{
#ifdef WIN32
	/*
	 * We cannot check the socket value at runtime because on Windows it can be
	 * greater than or equal to FD_SETSIZE.
	 */
	Assert(fds != NULL);
	return (fds->fd_count < FD_SETSIZE);
#else							/* !WIN32 */
	/*
	 * POSIX: the behavior of the macro FD_SET is undefined if the fd argument
	 * is less than 0 or greater than or equal to FD_SETSIZE.
	 */
	return (fd < FD_SETSIZE);
#endif							/* !WIN32 */
}

#endif							/* SOCKET_UTILS_H */
