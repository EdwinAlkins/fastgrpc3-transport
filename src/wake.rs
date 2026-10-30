//! The socket pair that wakes the asyncio loop when events are waiting. Unix: a Unix socket
//! pair, the loop watches its end (`add_reader`). Windows, whose Proactor loop cannot watch
//! a socket: a loopback TCP pair, the loop reads its end (`sock_recv`). Either way tokio
//! writes one byte when the queue goes from empty to non-empty, without the GIL.

#[cfg(unix)]
pub type Waker = std::os::unix::net::UnixStream;
#[cfg(windows)]
pub type Waker = std::net::TcpStream;

/// tokio's end, and Python's end as a raw descriptor (a `SOCKET` on Windows), both
/// non-blocking.
pub fn pair() -> std::io::Result<(Waker, i64)> {
    #[cfg(unix)]
    let (waker, python_end) = Waker::pair()?;
    #[cfg(windows)]
    let (waker, python_end) = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let waker = Waker::connect(listener.local_addr()?)?;
        let (python_end, peer) = listener.accept()?;
        // Another local process could have connected first.
        if peer != waker.local_addr()? {
            return Err(std::io::Error::other(
                "unexpected connection to the wake-up socket",
            ));
        }
        waker.set_nodelay(true)?;
        (waker, python_end)
    };
    waker.set_nonblocking(true)?;
    python_end.set_nonblocking(true)?;
    #[cfg(unix)]
    let raw = i64::from(std::os::fd::IntoRawFd::into_raw_fd(python_end));
    #[cfg(windows)]
    let raw = std::os::windows::io::IntoRawSocket::into_raw_socket(python_end) as i64;
    Ok((waker, raw))
}
