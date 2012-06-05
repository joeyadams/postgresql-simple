#include <stddef.h>

#if _WIN32
#include <winsock2.h>
#else
#include <errno.h>
#include <sys/select.h>
#endif

/*
 * Wait until the socket is ready, or until the timeout expires.  This may
 * return sooner, however, if the current OS thread is interrupted by a signal.
 *
 * Return values:
 *
 *  0: select() failed
 *
 *  1: select() was interrupted by a signal
 *
 *  2: Timeout expired
 *
 *  3: The socket is ready
 */
int c_postgresql_simple_wait_socket(int fd, int write, long sec, long usec)
{
    fd_set fds;
    struct timeval tv;
    int rc;

    FD_ZERO(&fds);
    FD_SET(fd, &fds);

    tv.tv_sec = sec;
    tv.tv_usec = usec;

    rc = select(fd + 1,
                write ? NULL : &fds,
                write ? &fds : NULL,
                NULL,
                &tv);
    if (rc < 0) {
        #if _WIN32
        switch (WSAGetLastError()) {
            case WSAEINTR:
                return 1;
            default:
                return 0;
        }
        #else
        switch (errno) {
            case EINTR:
                return 1;
            default:
                return 0;
        }
        #endif
    } else if (rc == 0) {
        return 2;
    } else {
        return 3;
    }
}
