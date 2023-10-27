-- Copyright 2016 Bloomberg Finance L.P.
-- All Rights Reserved.
-- This software is proprietary to Bloomberg Finance L.P. and is
-- provided solely under the terms of the BFLP license agreement.

CREATE TABLE Metadata(
	id INTEGER PRIMARY KEY,
	filename TEXT NOT NULL UNIQUE,
	update_time DATE NOT NULL,
	writer_pid INTEGER NOT NULL,
	pending INTEGER UNIQUE,
	error_message TEXT);

PRAGMA user_version = 1;
