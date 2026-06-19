#!/usr/bin/env python3
"""One-time helper: load the JT WatchList into a single existing account.

Run it once, from the same folder as app.py, so it uses the same database
(SQLite file, or DATABASE_URL if Postgres is configured):

    python3 seed_watchlist.py                       # uses the default email below
    python3 seed_watchlist.py someone@example.com   # or pass an email

It only ADDS the watchlist stocks to that account; it never deletes anything
and skips any symbol the account already has.
"""
import sys
import app   # reuses app.py's config + database helpers (does not start the server)

# Grandpa's account.
DEFAULT_EMAIL = "torresdjy@sbcglobal.net"


def main():
    email = (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_EMAIL).strip().lower()

    app.init_db()
    user = app.get_user_by_email(email)
    if not user:
        print(f"No account found for {email!r}. Make sure the email is correct "
              f"and that this script is pointed at the same database as the app.")
        sys.exit(1)
    uid = user[0]

    before = set(app.get_watchlist(uid))
    added, skipped = [], []
    for sym in app.DEFAULT_WATCHLIST:
        if sym in before:
            skipped.append(sym)
            continue
        if app.add_watch(uid, sym):
            added.append(sym)
        else:
            print(f"  ! could not add {sym} (watchlist limit is {app.MAX_PER_USER})")

    after = app.get_watchlist(uid)
    print(f"Account: {email}  (id {uid})")
    print(f"Added {len(added)}: {', '.join(added) or '—'}")
    if skipped:
        print(f"Already present, skipped {len(skipped)}: {', '.join(skipped)}")
    print(f"Watchlist now has {len(after)} stocks.")


if __name__ == "__main__":
    main()
