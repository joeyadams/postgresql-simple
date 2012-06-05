{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-}
module Database.PostgreSQL.Simple.Compat
    ( mask
    , mask_
    , threadWaitRead
    ) where

import Control.Concurrent       (forkIO, yield)
import Control.Concurrent.MVar
import Control.Monad            (join, when)
import Data.Function            (fix)
import Data.IORef
import Foreign.C.Types
import Network.Socket.Internal  (throwSocketError)
import System.Posix.Types       (Fd(..))

import qualified Control.Exception  as E
import qualified Control.Concurrent as C

-- | Like 'E.mask', but backported to base before version 4.3.0.
--
-- Note that the restore callback is monomorphic, unlike in 'E.mask'.  This
-- could be fixed by changing the type signature, but it would require us to
-- enable the RankNTypes extension (since 'E.mask' has a rank-3 type).  The
-- 'withTransactionMode' function calls the restore callback only once, so we
-- don't need that polymorphism.
mask :: ((IO a -> IO a) -> IO b) -> IO b
#if MIN_VERSION_base(4,3,0)
mask io = E.mask $ \restore -> io restore
#else
mask io = do
    b <- E.blocked
    E.block $ io $ \m -> if b then m else E.unblock m
#endif
{-# INLINE mask #-}

mask_ :: IO a -> IO a
#if MIN_VERSION_base(4,3,0)
mask_ = E.mask_
#else
mask_ io = mask $ \_ -> io
#endif

-- | 'C.threadWaitRead' does not work on Windows, at least not for sockets.
-- This works around the problem by repeatedly calling @select@ with a timeout.
threadWaitRead :: Fd -> IO ()
#if !mingw32_HOST_OS
threadWaitRead = C.threadWaitRead
#else
threadWaitRead = threadWaitSocket "threadWaitRead" WaitRead

threadWaitSocket :: String -> WaitFor -> Fd -> IO ()
threadWaitSocket errCtx wf fd = do
    caller_alive <- newIORef True
    result       <- newEmptyMVar

    mask_ $ do
        _ <- forkIO $ fix $ \loop -> do
            rc <- wait
            yield
            a <- readIORef caller_alive
            when a $ case rc of
                SelectInterrupted -> loop
                TimeoutExpired    -> loop
                SelectError       -> putMVar result $ throwSocketError errCtx
                SocketReady       -> putMVar result $ return ()

        join (takeMVar result `E.onException`
              atomicModifyIORef caller_alive (\_ -> (False, ())))
  where
    wait
        -- Use a long polling interval (5 seconds) when -threaded is enabled.
        -- The only penalty of a longer timeout is that if the caller receives
        -- an async exception, the OS thread busy with 'waitSocket' will dangle
        -- for up to the given length of time.
        | C.rtsSupportsBoundThreads = waitSocket fd wf 5 0

        -- When -threaded is disabled, an FFI call blocks the whole RTS,
        -- so use a very short polling interval (1/100 of a second).
        | otherwise = waitSocket fd wf 0 10000

data WaitFor
    = WaitRead
    | WaitWrite

data WaitResult
    = SelectError
    | SelectInterrupted
    | TimeoutExpired
    | SocketReady

-- | Strongly-typed wrapper around 'c_postgresql_simple_wait_socket'
waitSocket :: Fd -> WaitFor -> CLong -> CLong -> IO WaitResult
waitSocket (Fd fd) waitFor sec usec = do
    rc <- c_postgresql_simple_wait_socket fd (fromWaitFor waitFor) sec usec
    case rc of
        1 -> return SelectInterrupted
        2 -> return TimeoutExpired
        3 -> return SocketReady
        _ -> return SelectError
  where
    fromWaitFor WaitRead  = 0
    fromWaitFor WaitWrite = 1

foreign import ccall safe
    c_postgresql_simple_wait_socket
        :: CInt     -- ^ fd
        -> CInt     -- ^ 0 for read, 1 for write
        -> CLong    -- ^ sec
        -> CLong    -- ^ usec
        -> IO CInt  -- ^ See cbits/wait.c for the meaning of each error code

#endif
