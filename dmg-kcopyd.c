/*
 * Adapted from the original dm-kcopyd.c.
 *
 * Copyright (C) 2002 Sistina Software (UK) Limited.
 * Copyright (C) 2006 Red Hat GmbH
 *
 * This file is released under the GPL.
 *
 * Kcopyd provides a simple interface for copying an area of one
 * block-device to one or more other block-devices, with an asynchronous
 * completion notification.
 */

#include <linux/types.h>
#include <asm/atomic.h>
#include <linux/blkdev.h>
#include <linux/fs.h>
#include <linux/init.h>
#include <linux/list.h>
#include <linux/mempool.h>
#include <linux/module.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>
#include <linux/workqueue.h>
#include <linux/mutex.h>
#include <linux/version.h>
#include <linux/device-mapper.h>

#include <linux/dm-io.h>

#include "dmg-kcopyd.h"
#include "dm-gecko.h"

/*-----------------------------------------------------------------
 * Each kcopyd client has its own little pool of preallocated
 * pages for kcopyd io.
 *---------------------------------------------------------------*/
struct dmg_kcopyd_client {
	spinlock_t lock;
	struct page_list *pages;
	unsigned int nr_pages;
	unsigned int nr_free_pages;

	struct dm_io_client *io_client;

	wait_queue_head_t destroyq;
	atomic_t nr_jobs;

	mempool_t *job_pool;

	struct workqueue_struct *kcopyd_wq;
	struct work_struct kcopyd_work;

/*
 * We maintain three lists of jobs:
 *
 * i)   jobs waiting for pages
 * ii)  jobs that have pages, and are waiting for the io to be issued.
 * iii) jobs that have completed.
 *
 * All three of these are protected by job_lock.
 */
	spinlock_t job_lock;
	struct list_head complete_jobs;
	struct list_head io_jobs;
	struct list_head pages_jobs;
};

static inline void wake(struct dmg_kcopyd_client *kc)
{
	queue_work(kc->kcopyd_wq, &kc->kcopyd_work);
}

static struct page_list *alloc_pl(void)
{
	struct page_list *pl;

	pl = kmalloc(sizeof(*pl), GFP_KERNEL);
	if (!pl)
		return NULL;

	pl->page = alloc_page(GFP_KERNEL);
	if (!pl->page) {
		kfree(pl);
		return NULL;
	}

	return pl;
}

static inline void free_pl(struct page_list *pl)
{
	__free_page(pl->page);
	kfree(pl);
}

static int kcopyd_get_pages(struct dmg_kcopyd_client *kc,
			    unsigned int nr, struct page_list **pages)
{
	struct page_list *pl;

	spin_lock(&kc->lock);
	if (kc->nr_free_pages < nr) {
		printk("%s, somethings goes wrong here, no enough pages for work!!!!!!\n", __FUNCTION__);
		BUG_ON(1);
		spin_unlock(&kc->lock);
		return -ENOMEM;
	}

	kc->nr_free_pages -= nr;
	for (*pages = pl = kc->pages; --nr; pl = pl->next)  // zgy: allocate nr pages to *pages
		;

	kc->pages = pl->next;
	pl->next = NULL;

	//debug
	//printk("dmg-kcopyd: %s, %d free_pages before work\n", __FUNCTION__, kc->nr_free_pages);

	spin_unlock(&kc->lock);

	return 0;
}

static void kcopyd_put_pages(struct dmg_kcopyd_client *kc, struct page_list *pl)
{
	struct page_list *cursor;

	spin_lock(&kc->lock);
	for (cursor = pl; cursor->next; cursor = cursor->next)
		kc->nr_free_pages++;

	kc->nr_free_pages++;
	cursor->next = kc->pages;
	kc->pages = pl;

	//debug
	//printk("dmg-kcopyd: %s, %d free_pages after work\n", __FUNCTION__, kc->nr_free_pages);

	spin_unlock(&kc->lock);
}

/*
 * These three functions resize the page pool.
 */
static void drop_pages(struct page_list *pl)
{
	struct page_list *next;

	while (pl) {
		next = pl->next;
		free_pl(pl);
		pl = next;
	}
}

static int client_alloc_pages(struct dmg_kcopyd_client *kc, unsigned int nr)
{
	unsigned int i;
	struct page_list *pl = NULL, *next;

	for (i = 0; i < nr; i++) {
		next = alloc_pl();
		if (!next) {
			if (pl)
				drop_pages(pl);
			return -ENOMEM;
		}
		next->next = pl;
		pl = next;
	}

	kcopyd_put_pages(kc, pl);
	kc->nr_pages += nr;
	return 0;
}

static inline void client_free_pages(struct dmg_kcopyd_client *kc)
{
	BUG_ON(kc->nr_free_pages != kc->nr_pages);
	drop_pages(kc->pages);
	kc->pages = NULL;
	kc->nr_free_pages = kc->nr_pages = 0;
}

static struct kmem_cache *_job_cache;

int __init dmg_kcopyd_init(void)
{
	_job_cache = KMEM_CACHE(kcopyd_job, 0);
	if (!_job_cache)
		return -ENOMEM;

	return 0;
}

void dmg_kcopyd_exit(void)
{
	kmem_cache_destroy(_job_cache);
	_job_cache = NULL;
}

/*
 * Functions to push and pop a job onto the head of a given job
 * list.
 */
static struct kcopyd_job *pop(struct list_head *jobs,
			      struct dmg_kcopyd_client *kc)
{
	struct kcopyd_job *job = NULL;
	unsigned long flags;

	spin_lock_irqsave(&kc->job_lock, flags);

	if (!list_empty(jobs)) {
		job = list_entry(jobs->next, struct kcopyd_job, list);
		list_del(&job->list);
	}
	spin_unlock_irqrestore(&kc->job_lock, flags);

	return job;
}

static void push(struct list_head *jobs, struct kcopyd_job *job)
{
	unsigned long flags;
	struct dmg_kcopyd_client *kc = job->kc;

	spin_lock_irqsave(&kc->job_lock, flags);
	list_add_tail(&job->list, jobs);
	spin_unlock_irqrestore(&kc->job_lock, flags);
}


static void push_head(struct list_head *jobs, struct kcopyd_job *job)
{
	unsigned long flags;
	struct dmg_kcopyd_client *kc = job->kc;

	spin_lock_irqsave(&kc->job_lock, flags);
	list_add(&job->list, jobs);
	spin_unlock_irqrestore(&kc->job_lock, flags);
}

/*
 * These three functions process 1 item from the corresponding
 * job list.
 *
 * They return:
 * < 0: error
 *   0: success
 * > 0: can't process yet.
 */
static int run_complete_job(struct kcopyd_job *job)
{
	void *context = job->context;
	int read_err = job->read_err;
	unsigned long write_err = job->write_err;
	dmg_kcopyd_notify_fn fn = job->fn;  // ~seg_gc_complete_callback~ in dm-gecko.c
	struct dmg_kcopyd_client *kc = job->kc;

	//debug
	//static int count = 0;
	//printk("zgy: %s, called %d time(s)\n", __FUNCTION__, ++count);

	// TODO: check this error
	if (job->pages)
		kcopyd_put_pages(kc, job->pages);
	mempool_free(job, kc->job_pool);
	fn(read_err, write_err, context);

	if (atomic_dec_and_test(&kc->nr_jobs))
		wake_up(&kc->destroyq);

	return 0;
}

static void complete_io(unsigned long error, void *context)
{
	struct kcopyd_job *job = (struct kcopyd_job *) context;
	struct dmg_kcopyd_client *kc = job->kc;

	//debug
	//static int count = 0;

	if (error) {
		if (job->rw == WRITE)
			job->write_err |= error;
		else
			job->read_err = 1;

		if (!test_bit(DMG_KCOPYD_IGNORE_ERROR, &job->flags)) {
			push(&kc->complete_jobs, job);
			wake(kc);
			return;
		}
	}

	if (job->rw == WRITE) {
		push(&kc->complete_jobs, job);
	} else {
		job->rw = WRITE;
		push(&kc->io_jobs, job);
		if (job->readdone_fn != NULL) {
				(job->readdone_fn)(&job->num_dests,
									job->dests,
									job->context);
		}
	}

	wake(kc);
}

/*
 * Request io on as many buffer heads as we can currently get for
 * a particular job.
 */
static int run_io_job(struct kcopyd_job *job)
{
	int r;
	struct dmg_kcopyd_client *kc = job->kc;
	struct io_job *io = (struct io_job *)job->context;
	// debug
	//static int count = 1;

	struct dm_io_request io_req = {
		//.bi_rw = job->rw | (1 << BIO_RW_SYNCIO) | (1 << BIO_RW_UNPLUG),
		//.bi_rw = job->rw,
		// ziggy
		.bi_op = job->rw,
		.bi_op_flags = job->flags,
		.mem.type = DM_IO_PAGE_LIST,
		.mem.ptr.pl = job->pages,  // zgy: pages for IO
		.mem.offset = job->offset,
		.notify.fn = complete_io,  // zgy: ~complete_io~ change io->rw direction, also means this is a async IO
		.notify.context = job,
		.client = job->kc->io_client,
	};

	if (job->rw == READ) {
		r = dm_io(&io_req, 1, &job->source, NULL);  // async_io because we have notify.fn
	} else {
		if (job->readdone_fn_noirq != NULL) {
				(job->readdone_fn_noirq)(&job->num_dests,
											job->dests,
											job->context);
		}
		// zgy: after check, if there is no valid blocks, just stop here
		if (!io->nr_valid_blks){
			/* run_complete_job() will push the page back and call gc comeplete callback func */
			push(&kc->complete_jobs, job);

			return 0;
		}

		io_req.mem.ptr.pl = job->pages;  // reset this filed because we may change the pages list in readdone_fn_noirq()

		// job->dests == dm_io_region: has start sector and count
		r = dm_io(&io_req, job->num_dests, job->dests, NULL);
	}

	return r;
}

int DDDBUG = 1;
int COUNT = 0;
static int run_pages_job(struct kcopyd_job *job)
{
	int r;

	job->nr_pages = dm_div_up(job->source.count + job->offset,
				  PAGE_SIZE >> 9);

	r = kcopyd_get_pages(job->kc, job->nr_pages, &job->pages);  // zgy: get pages from free list for IO

	if (!r) {
		/* this job is ready for io */
		push(&job->kc->io_jobs, job);
		return 0;
	}

	if (r == -ENOMEM) {
		/* can't complete now */
		return 1;
	}

	return r;
}

/*
 * Run through a list for as long as possible.  Returns the count
 * of successful jobs.
 */
static int process_jobs(struct list_head *jobs, struct dmg_kcopyd_client *kc,
			int (*fn) (struct kcopyd_job *))
{
	struct kcopyd_job *job;
	int r, count = 0;

	while ((job = pop(jobs, kc))) {

		r = fn(job);

		if (r < 0) {
			/* error this rogue job */
			if (job->rw == WRITE)
				job->write_err = (unsigned long) -1L;
			else
				job->read_err = 1;
			push(&kc->complete_jobs, job);
			break;
		}

		if (r > 0) {
			/*
			 * We couldn't service this job ATM, so
			 * push this job back onto the list.
			 */
			push_head(jobs, job);
			break;
		}

		count++;
	}

	return count;
}

/*
 * kcopyd does this every time it's woken up.
 */
static void do_work(struct work_struct *work)
{
	struct dmg_kcopyd_client *kc = container_of(work,
												struct dmg_kcopyd_client,
												kcopyd_work);
	/*
	 * The order that these are called is *very* important.
	 * complete jobs can free some pages for pages jobs.
	 * Pages jobs when successful will jump onto the io jobs
	 * list.  io jobs call wake when they complete and it all
	 * starts again.
	 */
	process_jobs(&kc->complete_jobs, kc, run_complete_job);
	process_jobs(&kc->pages_jobs, kc, run_pages_job);
	process_jobs(&kc->io_jobs, kc, run_io_job);
}

/* running by dm-gecko GC workqueue thread */
static inline void dispatch_job(struct kcopyd_job *job)
{
	struct dmg_kcopyd_client *kc = job->kc;
	atomic_inc(&kc->nr_jobs);
	push(&kc->pages_jobs, job);
	wake(kc);
}

int dmg_kcopyd_copy(struct dmg_kcopyd_client *kc,
				struct dm_io_region *from,
				int num_dests,
				struct dm_io_region *dests,
				unsigned flags,
				dmg_kcopyd_notify_fn fn,
				void *context,
				//struct io_job *context,
				dmg_kcopyd_notify_readdone_fn readdone_fn,
				dmg_kcopyd_notify_readdone_fn_noirq readdone_fn_noirq)
{
	struct kcopyd_job *job;
	struct io_job *io;

	io = (struct io_job *)context;

	/*
	 * Allocate a new job.
	 */
	job = mempool_alloc(kc->job_pool, GFP_NOIO);

	/*
	 * set up for the read.
	 */
	job->kc = kc;
	job->flags = flags;
	job->read_err = 0;
	job->write_err = 0;
	job->rw = READ;

	job->source = *from;

	job->num_dests = num_dests;
	if (job->num_dests > 0) {
		memcpy(&job->dests, dests, sizeof(*dests) * num_dests);
	}
	job->offset = 0;
	job->nr_pages = 0;
	job->pages = NULL;

	job->fn = fn;  // gc_complete_callback
	job->context = context;  //zgy: contains dmg and v_blk, other fields are empty

	job->readdone_fn = readdone_fn;  // NULL
	job->readdone_fn_noirq = readdone_fn_noirq;  // we use this

	io->copy_job = job;  // container_of cannot help we use io to get kcopyd_job, so we bind it explicitly

	dispatch_job(job);
	return 0;
}

/*
 * Cancels a kcopyd job, eg. someone might be deactivating a
 * mirror.
 */
#if 0
int dmg_kcopyd_cancel(struct kcopyd_job *job, int block)
{
	/* FIXME: finish */
	return -1;
}
#endif  /*  0  */

/*-----------------------------------------------------------------
 * Client setup
 *---------------------------------------------------------------*/
// ziggy: kcopy initialization
int dmg_kcopyd_client_create(unsigned int nr_pages,
                             struct dmg_kcopyd_client **result)
{
	int r = -ENOMEM;
	struct dmg_kcopyd_client *kc;

	kc = kmalloc(sizeof(*kc), GFP_KERNEL);
	if (!kc)
		return -ENOMEM;

	spin_lock_init(&kc->lock);
	spin_lock_init(&kc->job_lock);
	INIT_LIST_HEAD(&kc->complete_jobs);
	INIT_LIST_HEAD(&kc->io_jobs);
	INIT_LIST_HEAD(&kc->pages_jobs);

	kc->job_pool = mempool_create_slab_pool(DMG_KCOPYD_MIN_JOBS,
                                                _job_cache);
	if (!kc->job_pool)
		goto bad_slab;

	INIT_WORK(&kc->kcopyd_work, do_work);
	kc->kcopyd_wq = create_singlethread_workqueue("dmg-kcopyd");
	if (!kc->kcopyd_wq)
		goto bad_workqueue;

	kc->pages = NULL;
	kc->nr_pages = kc->nr_free_pages = 0;
	r = client_alloc_pages(kc, nr_pages);
	if (r)
		goto bad_client_pages;
		
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 0, 0)
	kc->io_client = dm_io_client_create();
#else 
	kc->io_client = dm_io_client_create(nr_pages);
#endif
	if (IS_ERR(kc->io_client)) {
		r = PTR_ERR(kc->io_client);
		goto bad_io_client;
	}

	init_waitqueue_head(&kc->destroyq);
	atomic_set(&kc->nr_jobs, 0);

	*result = kc;
	return 0;

bad_io_client:
	client_free_pages(kc);
bad_client_pages:
	destroy_workqueue(kc->kcopyd_wq);
bad_workqueue:
	mempool_destroy(kc->job_pool);
bad_slab:
	kfree(kc);

	return r;
}

void dmg_kcopyd_client_destroy(struct dmg_kcopyd_client *kc)
{
	/* Wait for completion of all jobs submitted by this client. */
	wait_event(kc->destroyq, !atomic_read(&kc->nr_jobs));

	BUG_ON(!list_empty(&kc->complete_jobs));
	BUG_ON(!list_empty(&kc->io_jobs));
	BUG_ON(!list_empty(&kc->pages_jobs));
	destroy_workqueue(kc->kcopyd_wq);
	dm_io_client_destroy(kc->io_client);
	client_free_pages(kc);
	mempool_destroy(kc->job_pool);
	kfree(kc);
}
