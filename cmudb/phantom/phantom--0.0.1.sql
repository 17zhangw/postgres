CREATE FUNCTION phantom_start_worker(text[], int)
RETURNS int
LANGUAGE C VOLATILE
AS '$libdir/phantom', 'phantom_start_worker';

CREATE FUNCTION phantom_stop_worker()
RETURNS int
LANGUAGE C VOLATILE
AS '$libdir/phantom', 'phantom_stop_worker';

CREATE FUNCTION phantom_worker_exists()
RETURNS int
LANGUAGE C VOLATILE
AS '$libdir/phantom', 'phantom_stop_worker';
