{-# LANGUAGE RecordWildCards #-}
module Database.PostgreSQL.Simple.Cursor (
    -- * Cursor
    Cursor,
    declareCursor,
    declareCursor_,
    closeCursor,

    -- ** Cursor options
    CursorOptions(..),
    defaultCursorOptions,

    -- * Fetching rows
    fetchCursor,
    CursorDirection(..),
    moveCursor,
) where

import qualified Control.Exception as E
import qualified Data.ByteString.Char8 as B8
import Database.PostgreSQL.Simple.Compat ((<>))
import Database.PostgreSQL.Simple.Internal
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.Transaction (isFailedTransactionError)
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.Types
import {-# SOURCE #-} Database.PostgreSQL.Simple

import Data.Int (Int64)

data Cursor r = Cursor
    { cConnection :: !Connection
    , cName       :: !Query
    , cQuery_     :: !(Query -> IO [r])
    }

-- | Create a 'Cursor' for traversing the result set of a given query.
--
-- 'declareCursor' may only be used inside a transaction, unless 'cursorHold'
-- is set to 'True'.
declareCursor :: (ToRow params, FromRow row)
              => CursorOptions
              -> Connection
              -> Query
              -> params
              -> IO (Cursor row)
declareCursor opts conn query params = do
    query' <- formatQuery conn query params
    declareCursor_ opts conn (Query query')

-- | Variant of 'declareCursor' that does not perform query substitution.
declareCursor_ :: FromRow row
               => CursorOptions
               -> Connection
               -> Query
               -> IO (Cursor row)
declareCursor_ CursorOptions{..} conn query = do
    name <- newTempName conn
    _ <- execute_ conn $ "DECLARE " <> name <>
        (if cursorScroll then " SCROLL" else " NO SCROLL") <>
        (if cursorHold then " CURSOR WITH HOLD FOR " else " CURSOR FOR ") <>
        query
    return $! Cursor
        { cConnection = conn
        , cName       = name
        , cQuery_     = query_ conn
        }

-- | Close a cursor.
--
-- Cursors are closed automatically in the following situations:
--
--  * When the transaction that created it finishes, unless 'cursorHold'
--    was set to 'True' and the transaction completes successfully.
--
--  * When the database 'Connection' is closed.
closeCursor :: Cursor r -> IO ()
closeCursor Cursor{..} = do
    (execute_ cConnection ("CLOSE " <> cName) >> return ()) `E.catch` \e ->
        -- Don't throw exception if CLOSE failed because the transaction is
        -- aborted.  Otherwise, `bracket (declareCursor ...) closeCursor`
        -- will throw away the original error.
        if isFailedTransactionError e
            then return ()
            else E.throwIO e

data CursorOptions = CursorOptions
    { cursorScroll :: Bool
      -- ^ Default: 'False'
      --
      --   If 'True', allow retrieving rows in a non-sequential fashion
      --   This might impose a performance penalty.
    , cursorHold :: Bool
      -- ^ Default: 'False'
      --
      --   If 'True', retain the cursor after the transaction that created it
      --   successfully commits.  WARNING: this works by copying the rows to a
      --   temporary location on commit.
    }

-- | Defaults suitable for sequential traversal
defaultCursorOptions :: CursorOptions
defaultCursorOptions = CursorOptions
    { cursorScroll = False
    , cursorHold   = False
    }

data CursorDirection
  = Next
    -- ^ Fetch the next row.
  | Prior
    -- ^ Fetch the prior row.
  | Forward Int
    -- ^ Fetch the next N rows.  'Forward' 0 re-fetches the current row.
    --   'Forward' -N is the same as 'Backward' N.
  | Backward Int
    -- ^ Fetch the prior N rows.  'Backward' 0 re-fetches the current row.
    --   'Backward' -N is the same as 'Forward' N.
    --
    --   Note that fetching backwards returns results in reverse order.
  | ForwardAll
    -- ^ Fetch all remaining rows, leaving the cursor at the end.
  | BackwardAll
    -- ^ Fetch all prior rows, leaving the cursor at the beginning.
  | First
    -- ^ Fetch the first row (same as @'Absolute' 1@).
  | Last
    -- ^ Fetch the last row (same as @'Absolute' -1@).
  | Absolute Int
    -- ^ Move the cursor to the Nth row and fetch it.  If N is negative,
    --   use the |N|'th row from the end.  If N is out of range,
    --   position before first row or last row, and return an empty set.
    --
    --   @'Absolute' 0@ positions the cursor before the first row and returns
    --   an empty set.
  | Relative Int
    -- ^ Fetch the Nth succeeding row, or the |N|'th prior row if N is
    --   negative.  @'Relative' 0@ re-fetches the current row.

formatCursorDirection :: CursorDirection -> Query
formatCursorDirection dir = case dir of
    Next        -> "NEXT"
    Prior       -> "PRIOR"
    Forward n   -> "FORWARD " <> formatInt n
    Backward n  -> "BACKWARD " <> formatInt n
    ForwardAll  -> "FORWARD ALL"
    BackwardAll -> "BACKWARD ALL"
    First       -> "FIRST"
    Last        -> "LAST"
    Absolute n  -> "ABSOLUTE " <> formatInt n
    Relative n  -> "RELATIVE " <> formatInt n

formatInt :: Int -> Query
formatInt = Query . B8.pack . show
    -- FIXME: use a faster Int renderer.  Though the system call overhead of
    -- talking to the database probably dwarfs the overhead of 'formatInt'
    -- by a very large factor.

-- | Fetch rows from a cursor.
--
-- Warning: some modes (e.g. @'Backward' 5@) require the cursor to be created
-- with 'cursorScroll' set to 'True'.  When 'cursorScroll' is 'False'
-- (the default), only the following modes may be used:
--
--  * 'Next'
--
--  * @'Forward' n@, where n > 0.  (@'Forward' 0@ is not allowed.)
--
-- Other modes (e.g. 'ForwardAll', 'Relative' with positive n, 'Absolute'
-- with n greater than current position) may also work, but the PostgreSQL
-- documentation does not affirm this.
fetchCursor :: Cursor r -> CursorDirection -> IO [r]
fetchCursor Cursor{..} dir =
    cQuery_ $! "FETCH " <> formatCursorDirection dir <> " FROM " <> cName

-- | Like 'fetchCursor', but do not return rows, just move the cursor.
-- Return the number of rows that a 'fetchCursor' with the same parameters
-- would have returned.
moveCursor :: Cursor r -> CursorDirection -> IO Int64
moveCursor Cursor{..} dir =
    execute_ cConnection $!
        "MOVE " <> formatCursorDirection dir <> " IN " <> cName
