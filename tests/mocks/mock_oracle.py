"""
MockOracleClient — in-memory drop-in replacement for db/oracle_client.OracleClient.

Implements the same async interface.  Tests register fixture rows via
set_fixture() and optionally assert call history via assert_called_with().

No real Oracle connection is used.
"""
