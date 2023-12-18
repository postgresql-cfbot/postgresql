#include <sys/socket.h>

/*
 * Windows has gai_strerrorA(), but it is not thread-safe.
 *
 * These are the documented error values and the messages returned by the
 * system library (observed on Windows Server 2022), but our function returns
 * pointers to string constants for thread-safety.
 *
 * https://learn.microsoft.com/en-us/windows/win32/api/ws2tcpip/nf-ws2tcpip-getaddrinfo
 */
const char *
gai_strerror(int errcode)
{
	switch (errcode)
	{
		case EAI_AGAIN:
			return "This is usually a temporary error during hostname resolution and means that the local server did not receive a response from an authoritative server.";
		case EAI_BADFLAGS:
			return "An invalid argument was supplied.";
		case EAI_FAIL:
			return "A non-recoverable error occurred during a database lookup.";
		case EAI_FAMILY:
			return "An address incompatible with the requested protocol was used.";
		case EAI_MEMORY:
			return "Not enough memory resources are available to process this command.";
		case EAI_NONAME:
			return "No such host is known.";
		case EAI_SERVICE:
			return "The specified class was not found.";
		case EAI_SOCKTYPE:
			return "The support for the specified socket type does not exist in this address family.";
		default:
			return "Unknown server error";
	}
}
