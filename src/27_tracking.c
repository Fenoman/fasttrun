/*-------------------------------------------------------------------------
 *
 * src/27_tracking.c
 *      Tracking of CREATE TEMP TABLE: schedule, shared memory, persistence.
 *
 * What it holds.  The fasttrun.track_schedule parser and check, the shared
 * memory request and startup, and saving and loading the registry file.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/27_tracking.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/* ================================================================
 * Temp table creation tracking -- shared memory + ProcessUtility hook.
 *
 * Counts how often each temp table name is created across all
 * backends.  Used by fasttrun_prewarm() to pre-create only the
 * hottest tables at session init, instead of all 8000.
 *
 * Requires shared_preload_libraries for shmem allocation.
 * ================================================================ */

#define FASTTRUN_TRACK_MAX		8192
#define FASTTRUN_TRACK_FILE		"pg_stat/fasttrun_temp_stats"
#define FASTTRUN_TRACK_MAGIC	0x46545354	/* "FTST" */

typedef struct FasttrunTrackEntry
{
	char			relname[NAMEDATALEN];	/* hash key */
	int64			create_count;
	TimestampTz		last_create;
} FasttrunTrackEntry;

typedef struct FasttrunTrackSnapshot
{
	FasttrunTrackEntry *entries;
	int			count;
} FasttrunTrackSnapshot;

static HTAB			   *fasttrun_track_htab = NULL;
static LWLockId			fasttrun_track_lock;
static bool				fasttrun_track_enabled = false;
static int				fasttrun_prewarm_count = 1000;
/*
 * Re-entrancy guard: true while fasttrun_prewarm() runs its SPI create loop.
 * The CREATE TEMP TABLE (LIKE dummy.*) issued by create_temp_table re-enters
 * fasttrun_utility_hook.  Without this guard prewarm would count its own
 * creates as user creates and inflate its own top-N ranking.
 */
static bool				fasttrun_in_prewarm = false;
static char			   *fasttrun_prewarm_schema = "dummy_tmp";
static char			   *fasttrun_track_schedule = "mon-fri 08:00-18:00";
static ProcessUtility_hook_type prev_utility_hook = NULL;
static object_access_hook_type prev_object_access_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/*
 * Track schedule: tracking is only active inside the configured time
 * windows.  The default is "mon-fri 08:00-18:00".  An empty schedule
 * means always active.
 */
#define FASTTRUN_SCHED_MAX_WINDOWS 8

typedef struct
{
	uint8		day_mask;		/* bit 0=Sun, 1=Mon, ..., 6=Sat */
	uint16		start_min;		/* minutes since midnight */
	uint16		end_min;
} FasttrunSchedWindow;

static FasttrunSchedWindow fasttrun_sched_windows[FASTTRUN_SCHED_MAX_WINDOWS];
static int		fasttrun_sched_window_count = 0;
static pg_time_t fasttrun_sched_cache_minute = (pg_time_t) -1;
static bool		fasttrun_sched_cache_active = true;

/* Parse day token: "mon", "tue", ..., "sun". Returns 0-6 or -1 on error. */
static int
fasttrun_sched_parse_day(const char *tok, int len)
{
	static const char *days[7] = { "sun", "mon", "tue", "wed", "thu", "fri", "sat" };
	int			i;

	if (len != 3)
		return -1;
	for (i = 0; i < 7; i++)
		if (pg_strncasecmp(tok, days[i], 3) == 0)
			return i;
	return -1;
}

/* Parse "HH:MM" into minutes since midnight (0-1440). Returns -1 on error. */
static int
fasttrun_sched_parse_time(const char *s, const char *end)
{
	int			h = 0, m = 0;

	if (end - s != 5 || s[2] != ':')
		return -1;
	if (!isdigit((unsigned char) s[0]) || !isdigit((unsigned char) s[1]) ||
		!isdigit((unsigned char) s[3]) || !isdigit((unsigned char) s[4]))
		return -1;
	h = (s[0] - '0') * 10 + (s[1] - '0');
	m = (s[3] - '0') * 10 + (s[4] - '0');
	if (h > 24 || m > 59 || (h == 24 && m > 0))
		return -1;
	return h * 60 + m;
}

/*
 * Parse schedule string into out_windows[]. Returns number of windows,
 * or -1 on parse error. Empty string is valid (returns 0).
 *
 * Format: "mon-fri 08:00-18:00; sat 10:00-14:00"
 */
static int
fasttrun_sched_parse(const char *input,
					 FasttrunSchedWindow *out_windows,
					 int max_windows)
{
	const char *p = input;
	int			count = 0;

	if (input == NULL)
		return 0;

	/* Skip leading whitespace */
	while (*p && isspace((unsigned char) *p))
		p++;

	if (*p == '\0')
		return 0;

	while (*p)
	{
		FasttrunSchedWindow w = { 0, 0, 0 };
		const char *day_start = p;
		const char *day_end;
		const char *time_start;
		const char *time_end;
		const char *dash;
		int			start_min, end_min;

		if (count >= max_windows)
			return -1;				/* too many windows */

		/* Find end of day spec (first whitespace) */
		while (*p && !isspace((unsigned char) *p))
			p++;
		day_end = p;

		/* Skip whitespace between day and time */
		while (*p && isspace((unsigned char) *p) && *p != ';')
			p++;
		if (*p == '\0' || *p == ';')
			return -1;				/* missing time spec */

		time_start = p;
		while (*p && *p != ';')
			p++;
		time_end = p;
		/* Trim trailing whitespace from time */
		while (time_end > time_start && isspace((unsigned char) time_end[-1]))
			time_end--;

		/* Parse day spec: comma-separated list of days or day ranges */
		{
			const char *dp = day_start;

			while (dp < day_end)
			{
				const char *tok_start = dp;
				const char *tok_end;
				const char *day_dash;
				int			d1, d2, d;

				while (dp < day_end && *dp != ',')
					dp++;
				tok_end = dp;
				if (*dp == ',')
					dp++;

				day_dash = memchr(tok_start, '-', tok_end - tok_start);
				if (day_dash)
				{
					d1 = fasttrun_sched_parse_day(tok_start, day_dash - tok_start);
					d2 = fasttrun_sched_parse_day(day_dash + 1,
												  tok_end - day_dash - 1);
					if (d1 < 0 || d2 < 0)
						return -1;
					if (d1 <= d2)
					{
						for (d = d1; d <= d2; d++)
							w.day_mask |= (1 << d);
					}
					else
					{
						/* Wrap around: fri-mon means fri, sat, sun, mon */
						for (d = d1; d <= 6; d++)
							w.day_mask |= (1 << d);
						for (d = 0; d <= d2; d++)
							w.day_mask |= (1 << d);
					}
				}
				else
				{
					d1 = fasttrun_sched_parse_day(tok_start, tok_end - tok_start);
					if (d1 < 0)
						return -1;
					w.day_mask |= (1 << d1);
				}
			}
		}

		/* Parse time spec: HH:MM-HH:MM */
		dash = memchr(time_start, '-', time_end - time_start);
		if (!dash)
			return -1;
		start_min = fasttrun_sched_parse_time(time_start, dash);
		end_min = fasttrun_sched_parse_time(dash + 1, time_end);
		if (start_min < 0 || end_min < 0)
			return -1;
		if (end_min <= start_min)
			return -1;				/* no midnight crossing */

		w.start_min = (uint16) start_min;
		w.end_min = (uint16) end_min;
		out_windows[count++] = w;

		/* Skip window separator */
		if (*p == ';')
			p++;
		while (*p && isspace((unsigned char) *p))
			p++;
	}

	return count;
}

/* Return true if current local time is within any schedule window. */
static bool
fasttrun_schedule_is_active_now(void)
{
	pg_time_t	now;
	pg_time_t	cache_minute;
	struct pg_tm *tm;
	int			weekday, minute_of_day;
	int			i;

	if (fasttrun_sched_window_count == 0)
		return true;				/* no schedule = always active */

	now = (pg_time_t) time(NULL);
	cache_minute = now / 60;
	if (fasttrun_sched_cache_minute == cache_minute)
		return fasttrun_sched_cache_active;

	tm = pg_localtime(&now, log_timezone);
	if (tm == NULL)
	{
		fasttrun_sched_cache_minute = cache_minute;
		fasttrun_sched_cache_active = true;
		return true;				/* tz error -- fall back to active */
	}

	weekday = tm->tm_wday;			/* 0=Sunday */
	minute_of_day = tm->tm_hour * 60 + tm->tm_min;

	for (i = 0; i < fasttrun_sched_window_count; i++)
	{
		FasttrunSchedWindow *w = &fasttrun_sched_windows[i];

		if ((w->day_mask & (1 << weekday)) &&
			minute_of_day >= w->start_min &&
			minute_of_day < w->end_min)
		{
			fasttrun_sched_cache_minute = cache_minute;
			fasttrun_sched_cache_active = true;
			return true;
		}
	}

	fasttrun_sched_cache_minute = cache_minute;
	fasttrun_sched_cache_active = false;
	return false;
}

/* GUC assign hook: parse schedule string, apply to static state. */
static void
fasttrun_track_schedule_assign_hook(const char *newval, void *extra)
{
	FasttrunSchedWindow	parsed[FASTTRUN_SCHED_MAX_WINDOWS];
	int		n;

	n = fasttrun_sched_parse(newval, parsed, FASTTRUN_SCHED_MAX_WINDOWS);
	if (n < 0)
	{
		ereport(WARNING,
				(errmsg("fasttrun: invalid track_schedule \"%s\", tracking will stay always-on",
						newval)));
		fasttrun_sched_window_count = 0;
		fasttrun_sched_cache_minute = (pg_time_t) -1;
		fasttrun_sched_cache_active = true;
		return;
	}

	memcpy(fasttrun_sched_windows, parsed, sizeof(parsed));
	fasttrun_sched_window_count = n;
	fasttrun_sched_cache_minute = (pg_time_t) -1;
	fasttrun_sched_cache_active = true;
}

/* Shmem request hook -- called during postmaster startup. */
static void
fasttrun_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(hash_estimate_size(FASTTRUN_TRACK_MAX,
											  sizeof(FasttrunTrackEntry)));
	RequestAddinShmemSpace(fasttrun_shared_counters_size(MaxBackends));
	RequestNamedLWLockTranche("fasttrun", 1);
}

/* Save tracking stats to disk. Called at server shutdown. */
static void
fasttrun_track_save(int code, Datum arg)
{
	FILE			   *f = NULL;
	HASH_SEQ_STATUS		status;
	FasttrunTrackEntry *entry;
	int32				magic = FASTTRUN_TRACK_MAGIC;
	int32				count = 0;
	const char		   *phase = "track_save_open";
	int					save_errno = 0;
	bool				lock_held = false;
	bool				scan_active = false;
	bool				first_positive = true;

	/* Don't try to dump during a crash: shared state may be inconsistent. */
	if (code)
		return;

	if (fasttrun_track_htab == NULL)
		return;

	/*
	 * The postmaster has no PGPROC and cannot wait on an LWLock.  After a
	 * normal shutdown nobody holds the lock, but an immediate shutdown kills
	 * backends without releasing theirs, and the postmaster still exits
	 * through here with code 0.  A held lock also means the registry may be
	 * half-updated, so skip the save and keep the previous file.
	 */
	if (!LWLockConditionalAcquire(fasttrun_track_lock, LW_SHARED))
	{
		ereport(LOG,
				(errmsg("fasttrun: tracking statistics lock is held at shutdown, not saving")));
		return;
	}
	lock_held = true;

	f = AllocateFile(FASTTRUN_TRACK_FILE ".tmp", PG_BINARY_W);
	if (f == NULL)
		goto error;

	/* magic + count placeholder, count is rewritten below */
	phase = "track_save_header";
	if (fwrite(&magic, sizeof(magic), 1, f) != 1 ||
		fwrite(&count, sizeof(count), 1, f) != 1)
		goto error;

	hash_seq_init(&status, fasttrun_track_htab);
	scan_active = true;
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count > 0)
		{
			phase = "track_save_write";
			if (first_positive &&
				fasttrun_test_failpoint_matches("track_save_write", 0))
			{
				errno = ENOSPC;
				goto error;
			}
			first_positive = false;
			if (fwrite(entry, sizeof(FasttrunTrackEntry), 1, f) != 1)
				goto error;
			count++;
		}
	}
	scan_active = false;

	LWLockRelease(fasttrun_track_lock);
	lock_held = false;

	/* rewrite count over the placeholder */
	phase = "track_save_count";
	if (fseek(f, sizeof(magic), SEEK_SET) != 0 ||
		fwrite(&count, sizeof(count), 1, f) != 1)
		goto error;

	phase = "track_save_close";
	if (FreeFile(f))
	{
		f = NULL;
		goto error;
	}
	f = NULL;
	if (fasttrun_test_failpoint_matches("track_save_close", 0))
	{
		errno = EIO;
		goto error;
	}

	phase = "track_save_pre_rename";
	if (fasttrun_test_failpoint_matches("track_save_pre_rename", 0))
	{
		errno = EIO;
		goto error;
	}

	/* atomic replace: the old file stays intact if anything above failed */
	phase = "track_save_rename";
	if (durable_rename(FASTTRUN_TRACK_FILE ".tmp",
						   FASTTRUN_TRACK_FILE, LOG) != 0)
	{
		/* durable_rename does not preserve errno on return. */
		errno = EIO;
		goto error;
	}
	return;

error:
	save_errno = errno != 0 ? errno : EIO;
	if (scan_active)
		hash_seq_term(&status);
	if (lock_held)
		LWLockRelease(fasttrun_track_lock);
	if (f != NULL)
	{
		(void) FreeFile(f);
		f = NULL;
	}
	errno = save_errno;
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("fasttrun: could not save tracking statistics at phase %s: %m",
					phase)));
	(void) unlink(FASTTRUN_TRACK_FILE ".tmp");
}

/* Load tracking stats from disk. Called at shmem startup. */
static void
fasttrun_track_load(void)
{
	FILE			   *f;
	int32				magic, count;
	int					i;

	f = AllocateFile(FASTTRUN_TRACK_FILE, PG_BINARY_R);
	if (f == NULL)
		return;		/* no saved stats -- fresh start */

	if (fread(&magic, sizeof(magic), 1, f) != 1 || magic != FASTTRUN_TRACK_MAGIC)
	{
		FreeFile(f);
		return;
	}
	if (fread(&count, sizeof(count), 1, f) != 1 || count < 0)
	{
		FreeFile(f);
		return;
	}

	for (i = 0; i < count && i < FASTTRUN_TRACK_MAX; i++)
	{
		FasttrunTrackEntry	buf;
		FasttrunTrackEntry *entry;
		bool				found;

		if (fread(&buf, sizeof(buf), 1, f) != 1)
			break;

		entry = hash_search(fasttrun_track_htab, buf.relname,
							HASH_ENTER, &found);
		if (!found)
		{
			entry->create_count = buf.create_count;
			entry->last_create = buf.last_create;
		}
		else
		{
			entry->create_count += buf.create_count;
			if (buf.last_create > entry->last_create)
				entry->last_create = buf.last_create;
		}
	}

	FreeFile(f);
}

/* Shared memory startup: create hash table + load from disk. */
static void
fasttrun_shmem_startup(void)
{
	HASHCTL		ctl;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(FasttrunTrackEntry);

	fasttrun_track_htab = ShmemInitHash("fasttrun temp track",
										FASTTRUN_TRACK_MAX,
										FASTTRUN_TRACK_MAX,
										&ctl,
										HASH_ELEM | HASH_STRINGS);

	/* Allocate LWLock */
	fasttrun_track_lock = (LWLockId) GetNamedLWLockTranche("fasttrun");

	{
		bool		found;
		int			i;

		fasttrun_shared_counters = (FasttrunSharedCounters *)
			ShmemInitStruct("fasttrun counters",
							fasttrun_shared_counters_size(MaxBackends), &found);
		if (!found)
		{
			int			s;

			fasttrun_shared_counters->nslots = MaxBackends;
			for (s = 0; s < MaxBackends; s++)
				for (i = 0; i < FTC_COUNT; i++)
					pg_atomic_init_u64(&fasttrun_counter_slot(s)->values[i], 0);
		}
	}

	LWLockRelease(AddinShmemInitLock);

	/* Load saved stats */
	if (!IsUnderPostmaster)
	{
		fasttrun_track_load();
		on_shmem_exit(fasttrun_track_save, (Datum) 0);
	}
}

