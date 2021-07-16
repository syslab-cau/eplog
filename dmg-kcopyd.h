
#ifndef _GECKO_DMG_KCOPYD_H
#define _GECKO_DMG_KCOPYD_H

#include <linux/dm-io.h>

#include "dm-gecko.h"

#define DMG_KCOPYD_MAX_REGIONS  8
#define DMG_KCOPYD_MIN_JOBS     20
#define DMG_KCOPYD_IGNORE_ERROR 1
/*
 * To use kcopyd you must first create a dm_kcopyd_client object.
 */
struct dmg_kcopyd_client;
int dmg_kcopyd_client_create(unsigned num_pages,
                             struct dmg_kcopyd_client **result);
void dmg_kcopyd_client_destroy(struct dmg_kcopyd_client *kc);

/*
 * Submit a copy job to kcopyd.  This is built on top of the
 * previous three fns.
 *
 * read_err is a boolean,
 * write_err is a bitset, with 1 bit for each destination region
 */
typedef void (*dmg_kcopyd_notify_fn)(int read_err,
                                     unsigned long write_err,
                                     void *context);

typedef void (*dmg_kcopyd_notify_readdone_fn)(int *dst_count,
                                              struct dm_io_region *dst,
                                              void *context);

typedef void (*dmg_kcopyd_notify_readdone_fn_noirq)(int *dst_count,
                                                    struct dm_io_region *dst,
                                                    void *context);

/* @context is used for all callbacks */
int dmg_kcopyd_copy(struct dmg_kcopyd_client *kc,
                    struct dm_io_region *from,
                    int num_dests,
                    struct dm_io_region *dests,
                    unsigned flags,
                    dmg_kcopyd_notify_fn fn,
					void *context,
					//struct io_job *context,
                    dmg_kcopyd_notify_readdone_fn readdone_fn,
                    dmg_kcopyd_notify_readdone_fn_noirq readdone_fn_noirq);

int dmg_kcopyd_init(void);
void dmg_kcopyd_exit(void);

/*-----------------------------------------------------------------
 * kcopyd_jobs need to be allocated by the *clients* of kcopyd,
 * for this reason we use a mempool to prevent the client from
 * ever having to do io (which could cause a deadlock).
 *---------------------------------------------------------------*/
struct kcopyd_job {
	struct dmg_kcopyd_client *kc;
	struct list_head list;
	unsigned long flags;

	/*
	 * Error state of the job.
	 */
	int read_err;
	unsigned long write_err;

	/*
	 * Either READ or WRITE
	 */
	int rw;
	struct dm_io_region source;

	/*
	 * The destinations for the transfer.
	 */
	int num_dests;
	struct dm_io_region dests[DMG_KCOPYD_MAX_REGIONS];

	sector_t offset;
	unsigned int nr_pages;
	struct page_list *pages;

	/*
	 * Set this to ensure you are notified when the job has
	 * completed.  'context' is for callback to use.
	 */
	dmg_kcopyd_notify_fn fn;
	void *context;
	//struct io_job *context;

	dmg_kcopyd_notify_readdone_fn readdone_fn;
	dmg_kcopyd_notify_readdone_fn_noirq readdone_fn_noirq;
};
#endif  /* _GECKO_DMG_KCOPYD_H */
