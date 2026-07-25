"""WalFlux: incrementally-maintained materialized views for Postgres.

WalFlux tails a Postgres logical replication slot (pgoutput protocol) and
keeps aggregate tables fresh within milliseconds of each committed source
transaction — exactly once, even across crashes.
"""

__version__ = "0.1.0"
