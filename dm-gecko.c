/*
 * Copyright (C) 2011 Tudor Marian <tudorm@cs0.cornell.edu> (see
 * LICENSE file)
 */

#include <asm/atomic.h>
//#include <asm/uaccess.h>
#include <linux/uaccess.h>
#include <asm/unistd.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/device-mapper.h>
#include <linux/dm-io.h>
#include <linux/fcntl.h>
#include <linux/file.h>
#include <linux/fs.h>
#include <linux/hash.h>
#include <linux/highmem.h>
#include <linux/hrtimer.h>
#include <linux/init.h>
// dm-tx

// #include <linxu/ioctl.h>
#include <linux/kernel.h>
#include <linux/kthread.h>
#include <linux/list.h>
#include <linux/mempool.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/pagemap.h>
#include <linux/percpu.h>

#include <linux/random.h>
#include <linux/rwsem.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/syscalls.h>
#include <linux/version.h>
#include <linux/workqueue.h>
#include <scsi/sg.h>

#include "dmg-kcopyd.h"

// ziggy: for cleaning
#include <linux/sort.h>
// ziggy: TEST
#include <asm/segment.h>
#include <linux/buffer_head.h>

#include "calclock.h"
/* zgy: some functions only for testing use */
unsigned long long gc_spin_time = 0, gc_spin_count = 0;

unsigned long long gc_running_time = 0, gc_running_count = 0;
unsigned long long total_rw_time = 0, total_rw_count = 0;
unsigned long long rw_dmglock_time = 0, rw_dmglock_count = 0;
unsigned long long total_gc_time = 0;

unsigned long long dev_gc_tar_time = 0, dev_gc_tar_cnt = 0;
unsigned long long dispatch_dmglock_time = 0, dispatch_seg_cnt = 0;
unsigned long long gc_read_dmglock_time = 0, gc_read_callback_cnt = 0;
unsigned long long gc_complete_dmglock_time = 0, gc_complete_callback_cnt = 0;
unsigned long long gc_complete_joblock_time = 0, gc_complete_joblock_cnt = 0;
unsigned long long dest_blk_for_gc_time = 0, dest_blk_for_gc_cnt = 0;  // part of the dispatch_seg_time
 // profiling
struct timespec64 gc_clock[2];
struct timespec64 rw_clock[2];

struct seg_gc_page_position_list {
        int position;
        struct list_head list;
};

/* For segment gc relocate next free segment for a cleaned device */
enum FIRST_SEG {not, is};
enum FIRST_SEG IS_FIRST_SEG;

//#define ALWAYS_RUN_GC
//#define DROP_WRITE_WRITE_CLASH_OPTIMIZATION
//#define SYS_RENAME_EXPORTED_TO_MODULES

//#define MAX_DETAIL_LOG_LOOP_CNT 0xffffffff
#define MAX_DETAIL_LOG_LOOP_CNT 8

#define MIN_JOBS_IN_POOL        512
#define DM_GECKO_GC_COPY_PAGES  5120
//#define DM_GECKO_GC_COPY_PAGES  133120  // there are 512 io_job objects, each need 256 pages (at least 131,072 pages)
#define DM_GECKO_MAX_STRIPES    DMG_KCOPYD_MAX_REGIONS
#define MIN_GC_CONCURRENT_REQ   4
#define GC_CONCURRENT_REQ       64
#define MAX_GC_CONCURRENT_REQ   DM_GECKO_GC_COPY_PAGES

#define GC_DEFAULT_LOW_WATERMARK         0
#define GC_DEFAULT_HIGH_WATERMARK        3
#define DM_GECKO_CRITICAL_WATERMARK      1024
#define DM_GECKO_CRITICAL_WATERMARK_HARD 8


//ziggy: TEST
#define DM_GECKO_DEBUG 0
#define DM_GECKO_PREFIX "dm-gecko: "
#if DM_GECKO_DEBUG
#define DPRINTK( s, arg... ) printk(DM_GECKO_PREFIX s "\n", ##arg)
#else

#define DPRINTK( s, arg... )
#endif

#define ZGY_DEBUG 0
#define ZGY_PROFILING 0

#define DM_GECKO_ZIGGY "ziggy: "
#if ZGY_DEBUG
#define ZDBG(msg, args...) do { \
    printk(DM_GECKO_ZIGGY msg, ##args); \
} while (0)
#else
#define ZDBG(msg, args...) do {} while (0)
#endif

#define GECKO_TIMER_PERIOD_SECS         (1)
#define GECKO_TIMER_PERIOD_NSECS        (0)

#define GECKO_BLOCK_SHIFT PAGE_SHIFT        /* same as the size of pages */
#define GECKO_BLOCK_SIZE (1UL << GECKO_BLOCK_SHIFT)   // ziggy: 4096 bytes

#define GECKO_SECTOR_TO_BLOCK_SHIFT (GECKO_BLOCK_SHIFT - SECTOR_SHIFT)  // 3 bits
#define GECKO_SECTOR_TO_BLOCK_MASK ((1UL << GECKO_SECTOR_TO_BLOCK_SHIFT) - 1)
#define GECKO_SECTORS_PER_BLOCK (1UL << GECKO_SECTOR_TO_BLOCK_SHIFT)
/*  Dev (ziggy): get segments */
#define CLEAN_BLOCK_TO_SEGMENT (8)  // Set segment to 1MB, each block is 4KB
#define CLEAN_SECTOR_TO_SEGMENT (GECKO_SECTOR_TO_BLOCK_SHIFT + CLEAN_BLOCK_TO_SEGMENT)
/* For segment mechanism */
#define BLOCKS_PER_SEGMENT 256
//#define BLOCKS_PER_SEGMENT 512
#define NR_CLEAN_SEGS 10240
//#define NR_CLEAN_SEGS 5120
#define CLEAN_SECTORS_PER_SEG (1UL << CLEAN_SECTOR_TO_SEGMENT)
#define SEG_TO_ZONE_SHIFT 8  // assume the zone size is 256MB, and each segment is 1MB

/* Hashtable for pending IO operations indexed by block number */
#define HASH_TABLE_BITS 12
//#define HASH_TABLE_BITS 16
#define HASH_TABLE_SIZE (1UL << HASH_TABLE_BITS)

/******************************************************************

 * Sector and block operations
 *****************************************************************/
static inline u32 sector_to_block(sector_t sector)
{
        return (sector >> GECKO_SECTOR_TO_BLOCK_SHIFT);
}

static inline sector_t block_to_sector(u32 block)
{
        return (block << GECKO_SECTOR_TO_BLOCK_SHIFT);
}


static inline int sector_at_block_boundary(sector_t sector)
{
        return ((sector & GECKO_SECTOR_TO_BLOCK_MASK) == 0x0);
}

static inline int bio_start_at_block_boundary(struct bio *bio)
{
        return sector_at_block_boundary(bio->bi_iter.bi_sector);
}

static inline int bio_end_at_block_boundary(struct bio *bio)
{
        return sector_at_block_boundary(bio->bi_iter.bi_sector +
                                        to_sector(bio->bi_iter.bi_size));
}

static inline int bio_at_block_boundary(struct bio *bio)
{
        return bio_start_at_block_boundary(bio)
            && bio_end_at_block_boundary(bio);
}

static inline int bio_single_block_at_block_boundary(struct bio *bio)
{
        return (bio->bi_iter.bi_size == GECKO_BLOCK_SIZE)
            && bio_at_block_boundary(bio);
}

/*  Dev (ziggy) */
static inline unsigned int sector_to_segment(sector_t sector)
{
        return (sector >> CLEAN_SECTOR_TO_SEGMENT);
}


static inline int is_last_block_in_seg(unsigned short itr)

{
        return itr == (BLOCKS_PER_SEGMENT - 1);
}

// ziggy: Know status of the disk (maybe the mirror disks)
enum seg_power_state {
        unspecified,
        active,      /* normal active/idle operation mode */
        standby,     /* low power mode, drive has spun down */
        sleep,       /* lowest power mode, drive is completely shut down */
};

#define DEFAULT_LOW_POW_STATE standby


struct dm_gecko;

/*  Dev(ziggy): New structure for quickly get temperature of segments */
struct dev_sit_entry {
        unsigned int seg_nr;  // index of segments
        unsigned int temperature;  // calculate using jones-2016
        unsigned short itr;  // the iterator that point to active block
        //unsigned short nr_invalid_blk;
        atomic_t nr_invalid_blk;
        struct list_head list;
        u32 *r_map;  // the mapping information of the blocks in this segment
};

/* A SMR disk are divided into several fixed-sized zone */
struct smr_zone {
        int zone_idx;
        unsigned int size;  // in number of segments
        atomic_t nr_invalid_blk;
};

struct dm_dev_seg {
        struct list_head list;
        int idx;          /* only used for debugging purposes */
        sector_t start;   /* start offset in sectors related to the whole chain */
        sector_t len;     /* len in sectors */
        struct dm_dev *dev[DM_GECKO_MAX_STRIPES];
        struct work_struct work;
        enum seg_power_state cur_pow_state, next_pow_state;
        unsigned long long access_seq_in_log;  // version number. for device switch
        atomic_t pending_writes;
        atomic_t pending_reads;
        struct dm_gecko *ctxt;

        /* fields for segment management */
        struct work_struct cleaning_work;  // GC work
        unsigned int free_seg_nr;  // # of free segments in this device.
        unsigned int total_seg_nr;
        struct dev_sit_entry *curr_seg;  // current segment that handles write requests.
        struct dev_sit_entry **sit;  // segment information table of this device. (array of *dev_sit_entry)

        struct dev_sit_entry free_list;  // a list of free segments.

        struct smr_zone *zones;
};

enum dm_gecko_layout { linear, raid0, raid1, raid5, raid6 };

struct phy_disk_map {
        sector_t len;                /* total linear length in sectors */
        enum dm_gecko_layout layout;
        int stripes;
        int cnt;                    /* number of blk devs */
        struct list_head dm_dev_segs;
};

struct dm_gecko_stats {
        unsigned long long reads, subblock_reads, writes, subblock_writes,
            gc, discards, dropped_discards, empty_barriers, gc_recycle,
            rw_clash, rw_gc_clash, gc_clash, gc_rw_clash, ww_clash,
            read_empty, read_err, write_err, kcopyd_err, sb_read, sb_write;
};

struct gc_ctrl {
        u32 low_watermark;
        u32 high_watermark;
};

enum {  // dm_gecko->flags bit positions
        DM_GECKO_GC_FORCE_STOP,
	DM_GECKO_FINAL_SYNC_METADATA,
	DM_GECKO_GC_STARTED,
	DM_GECKO_READ_TPUT,
	DM_GECKO_INDEPENDENT_GC,
	DM_GECKO_STATUS_DETAILED,
	DM_GECKO_SYNCING_METADATA,
};

struct dm_gecko {
        spinlock_t lock;
        spinlock_t write_dev_lock; /* ziggy: lock for block_from_seg */
        atomic_t total_jobs;    /* used to safely destroy the target */
        struct list_head *buckets;  // a hash table for tracking reqs on each v_blk
        spinlock_t *htable_entry_locks;  // each htable entry has its own lock
        atomic_t htable_size;
        u32 *d_map;             /* direct map */
        //u32 *r_map;             /* reversed map */
        u32 tail;
        u32 persistent_tail;

        u32 head;
        u32 size;                /* size of the maps in number of blocks */
        /* free blocks that can be used w/o reclaiming initialized to
           ->size.  Circular ring logic dictates that available_blocks
           must be > 1 */
        u32 persistent_available_blocks;
       // u32 available_blocks;  /* free blocks that can be used directly */
        //u32 free_blocks;        /* total number of free blocks */
        atomic_t available_blocks;  /* free blocks that can be used directly */
        atomic_t free_blocks;        /* total number of free blocks */
        struct dm_dev_seg *head_seg;
        struct dm_dev_seg *tail_seg;
        volatile unsigned long flags;
        atomic_t gc_wait_for_req;
        int max_gc_req_in_progress;
        struct phy_disk_map disk_map;
        struct dmg_kcopyd_client *kcopyd_client;
        struct dm_io_client *io_client;
        struct dm_gecko_stats *stats;
        struct gc_ctrl gc_ctrl;
        struct hrtimer timer;
        ktime_t timer_delay;
        struct work_struct gc_work;
        atomic_t timer_active;
        atomic_t gc_work_scheduled_by_timer;
        atomic_t nr_kcopyd_jobs;
        struct work_struct sync_metadata_work;
        wait_queue_head_t jobs_pending_waitqueue;
        wait_queue_head_t no_free_space_waitqueue;
        wait_queue_head_t pending_write_waitqueue;  /* for gc */
        wait_queue_head_t kcopyd_job_waitqueue; /* the # of kcopyd job has limitation */
        wait_queue_head_t gc_waitqueue; /* let GC wait for io on target v_blk finish first */
        wait_queue_head_t gc_pending_read_waitqueue; /* let GC wait for READ to finish first (preempty) */
        struct rw_semaphore metadata_sync_sema;
        enum seg_power_state low_pow_state;
        unsigned long long incarnation;
        unsigned long tail_wrap_around;
        unsigned long head_wrap_around;
        char *meta_filename;

        //unsigned int total_free_seg_nr;  // Total # of free segments in all devices. Used for GC.
        unsigned short writing_dev;  // tracks the index of current working disk.
};

struct dm_gecko_dev {
        char name[16];  // for the persistent metadata representation
};

/* TUDORICA, the diminutive form of my name (Tudor) in Romanian ;) */
#define DM_GECKO_META_MAGIC (0x2D0031CA)

/* the persistent dm_gecko starts w/ this header on disk, followed by
   all the disk_map_cnt device names of (struct dm_gecko_dev) type,
   and by block maps */
struct dm_gecko_persistent_metadata {
        unsigned long long incarnation;
        u32 magic;
        u32 size;
        u32 tail;
        u32 head;
        u32 available_blocks;
        u32 free_blocks;
        unsigned long flags;
        int max_gc_req_in_progress;
        enum dm_gecko_layout layout;
        int stripes;
        int disk_map_cnt;
        struct gc_ctrl gc_ctrl;
        enum seg_power_state low_pow_state;
};

struct io_for_block {
        sector_t key;
        int rw_cnt;                     /* number of IOs in progress. If
                                         * negative, the gc is running */
        int seg_idx;                    /* for indicating last block in the victim seg */
        int gc_while_pending;             /* for indicating if there are pending requests on the v_blk when we do GC on it.
                                           * We can not use rw_cnt to indicate gc when there are pending requests. */

#define WRITE_CLASH_IO_FOR_BLOCK 0
        volatile unsigned long flags;   /* for write_clash optimization */
        struct list_head hashtable;     /* chain into the hash table */
        struct list_head pending_io;    /* list of IOs in progress */
        struct list_head deferred_io;   /* list of deferred IOs */
        spinlock_t lock;  /* ziggy:  */
};

//struct io_job {
//        struct list_head list;
//        struct dm_gecko *dmg;
//        int rw;                        /* READ or WRITE */
//        struct io_for_block *parent;   /* if NULL, the job is deferred */
//        void *page;                    /* for read-modify-update cycles */
//        struct bio *bio;               /* if NULL this is a gc IO */
//        sector_t v_block;              /* virtual block */
//        sector_t l_block;              /* linear block */
//        //u32 v_block;              /* virtual block */
//        //u32 l_block;              /* linear block */
//        /* for whole segment gc */
//        struct dm_dev_seg *dest_dev;  /*zgy: only for gc kcopyd callback*/
//        unsigned int seg_idx;  /*zgy: only for gc kcopyd callback*/
//        struct valid_blks_info *blks_info;  /* array */
//        unsigned int nr_valid_blks;
//};

struct valid_blks_info {
        u32 v_blk;
        u32 l_blk;
};

static inline int sector_in_seg(sector_t sector, struct dm_dev_seg *seg)
{
        return (sector >= seg->start) && (sector < seg->start + seg->len);
}

// ziggy: pass the sector, get the corresponding device
static struct dm_dev_seg *sector_to_dev(struct dm_gecko *dmg,
                                                sector_t sector)
{
        struct dm_dev_seg *seg;
        list_for_each_entry(seg, &dmg->disk_map.dm_dev_segs, list) {
                if (sector < seg->start + seg->len) {
                        return seg;
                }
        }
        return NULL;
}

static struct dm_dev_seg *dev_for_idx(struct dm_gecko *dmg,
                                                unsigned short idx)
{
        struct dm_dev_seg *dev;
        list_for_each_entry(dev, &dmg->disk_map.dm_dev_segs, list) {
                if (dev->idx == idx) {
                        return dev;
                }
        }
        return NULL;
}

static inline struct dev_sit_entry *sit_for_blk(struct dm_gecko *dmg, u32 l_blk)
{
        sector_t sector;
        sector_t sec_in_dev;
        struct dm_dev_seg *dev;
        int seg_idx;

        sector = block_to_sector(l_blk);
        dev = sector_to_dev(dmg, sector);
        sec_in_dev = sector - dev->start;
        seg_idx = sector_to_segment(sec_in_dev);

        return dev->sit[seg_idx];
}

/* get_rmap_element - access r_map in segment
 *
 * @l_blk: the unique linear block number
 *
 * Get the mapped v_block of the l_blk.
 */
static inline u32 get_rmap_element(struct dm_gecko *dmg, u32 l_blk)
{
        sector_t sector;
        sector_t sec_in_dev;
        struct dm_dev_seg *dev;
        int seg_idx;
        u32 blk_idx;

        sector = block_to_sector(l_blk);
        dev = sector_to_dev(dmg, sector);
        sec_in_dev = sector - dev->start;
        seg_idx = sector_to_segment(sec_in_dev);
        blk_idx = sector_to_block(sec_in_dev) % BLOCKS_PER_SEGMENT;

        return dev->sit[seg_idx]->r_map[blk_idx];
}

/* if the l_blk is unique, we do not need to use lock */
static inline void update_rmap(struct dm_gecko *dmg, u32 l_blk, u32 v_blk)
{
        sector_t sector;
        sector_t sec_in_dev;
        struct dm_dev_seg *dev;
        int seg_idx;
        u32 blk_idx;

        sector = block_to_sector(l_blk);
        dev = sector_to_dev(dmg, sector);
        sec_in_dev = sector - dev->start;
        seg_idx = sector_to_segment(sec_in_dev);
        blk_idx = sector_to_block(sec_in_dev) % BLOCKS_PER_SEGMENT;

        dev->sit[seg_idx]->r_map[blk_idx] = v_blk;
}

// ziggy: find where to RW (real device)
static inline void linear_to_phy_raid0(struct dm_gecko *dmg,
				       struct dm_dev_seg *seg,
				       sector_t sector,
				       struct dm_io_region *where)
{
    int stripe;
	sector_t block;
	sector -= seg->start;  // ziggy: get offset
	block = sector_to_block(sector);
	// The do_div is a macro that updates @block with the
	// quotient and returns the remainder. Use block instead
	// of the sector, since all sectors, being block aligned,
	// are even, so the stripe is always 0.
	stripe = do_div(block, dmg->disk_map.stripes);
	where->bdev = seg->dev[stripe]->bdev;
	where->sector = block_to_sector(block);
	where->count = GECKO_SECTORS_PER_BLOCK;
}

static struct dm_dev_seg *linear_to_phy_all(struct dm_gecko *dmg,
					    sector_t sector,
					    struct dm_io_region *where,
                                            int *num_regions)
{
        int i;
        struct dm_dev_seg *seg = sector_to_dev(dmg, sector);

        BUG_ON(!seg);  /* must fit in the range somewhere */

	if (dmg->disk_map.layout == raid0) {
		linear_to_phy_raid0(dmg, seg, sector, &where[0]);
		*num_regions = 1;
	} else {
        for (i = 0; i < dmg->disk_map.stripes; i++) {
            where[i].bdev = seg->dev[i]->bdev;
            where[i].sector = sector - seg->start;
            where[i].count = GECKO_SECTORS_PER_BLOCK;
		}
        *num_regions = dmg->disk_map.stripes;
	}
        return seg;
}

static struct dm_dev_seg *linear_to_phy_which(struct dm_gecko *dmg,
					      sector_t sector,
					      unsigned which,
					      struct dm_io_region *where)
{
        struct dm_dev_seg *seg = sector_to_dev(dmg, sector);

        BUG_ON(!seg);  /* must fit in the range somewhere */
        BUG_ON(which >= dmg->disk_map.stripes);

	if (dmg->disk_map.layout == raid0) {
		linear_to_phy_raid0(dmg, seg, sector, where);
	} else {
	        where->bdev = seg->dev[which]->bdev;  // TODO(ziggy): This is a strange code. Why #request bind with the target device. there is no reason.
		where->sector = sector - seg->start;
		where->count = GECKO_SECTORS_PER_BLOCK;
	}
        return seg;
}

static inline struct dm_dev_seg *linear_to_phy_which_read_seg(struct dm_gecko *dmg,
					      sector_t sector,
					      unsigned which,
					      struct dm_io_region *where)
{
        struct dm_dev_seg *seg = sector_to_dev(dmg, sector);

        BUG_ON(!seg);  /* must fit in the range somewhere */
        BUG_ON(which >= dmg->disk_map.stripes);

	if (dmg->disk_map.layout == raid0) {
		linear_to_phy_raid0(dmg, seg, sector, where);
	} else {
	        where->bdev = seg->dev[which]->bdev;  // in our code, which is always 0, because we only have 1 disk per unit
		where->sector = sector - seg->start;
		where->count = CLEAN_SECTORS_PER_SEG;  // # of sector per segment
	}
        return seg;
}

static inline u32 mark_block_free(struct dm_gecko *dmg)
{
        return dmg->size;
}

static inline int is_block_marked_free(u32 block, struct dm_gecko *dmg)
{
        return (block == dmg->size);
}

static inline int is_block_invalid(u32 block, struct dm_gecko *dmg)
{
        return (block > dmg->size);
}

static inline int is_block_free_or_invalid(u32 block, struct dm_gecko *dmg)
{
        return (block >= dmg->size);
}

static inline int __no_available_blocks(struct dm_gecko *dmg)
{
        /* can be less than the watermark temporarily while gc runs */
        return (atomic_read(&dmg->available_blocks) <= DM_GECKO_CRITICAL_WATERMARK);
}

static inline int __no_available_blocks_hard(struct dm_gecko *dmg)
{
        return (atomic_read(&dmg->available_blocks) <= DM_GECKO_CRITICAL_WATERMARK_HARD);
}

/* used by all dm-gecko targets */
static DEFINE_SPINLOCK(jobs_lock);
/* the workqueue picks up items off this list */
static LIST_HEAD(deferred_jobs);

/* mempool cache allocators */
static struct kmem_cache *io_for_block_cache, *io_job_cache;
static mempool_t *io_for_block_mempool, *io_job_mempool;

/* Deferred work and work that needs a task context executes on this
 * workqueue. Must be singlethreaded. */
static struct workqueue_struct *gecko_wqueue = NULL;
static struct work_struct gecko_work;
/* This workqueue is used only to sync the metadata from a
task-context. Trying to use the same gecko_wqueue for this operation
would render the deadlock avoidance logic unnecessarily complicated. */
static struct workqueue_struct *gecko_sync_metadata_wqueue = NULL;

/* ziggy: This workqueue is used only for cleaning */
static struct workqueue_struct *gecko_cleaning_wqueue = NULL;

struct deferred_stats {
        unsigned long long gc, rw, total;
};
DEFINE_PER_CPU(struct deferred_stats, deferred_stats);

static  void do_complete_generic(struct dm_gecko *dmg)
{
        if (atomic_dec_and_test(&dmg->total_jobs)) {
                wake_up(&dmg->jobs_pending_waitqueue);
        }
}

static void do_run_gc(struct io_job *io);
static void map_rw_io_job(struct io_job *io);

static inline void wake_deferred_wqueue(void)
{
        queue_work(gecko_wqueue, &gecko_work);  // ziggy: Put the work into the work queue.must be singlethreaded
}

static inline int io_job_is_deferred(struct io_job *io)
{
        return (io->parent == NULL);
}

static inline void set_io_job_deferred(struct io_job *io)
{
        io->parent = NULL;
}

static inline int io_job_is_gc(struct io_job *io)
{
        return (io->bio == NULL);
}

static inline void set_io_job_gc(struct io_job *io)
{
        io->bio = NULL;
}

static inline void __add_deferred_io_job(struct io_job *io)
{
        set_io_job_deferred(io);
        list_add_tail(&io->list, &deferred_jobs);
}

/*
 * Watch out! the waked workqueue will lock jobs_lock and call map_rw_io_job,
 * which try to hold dmg->lock.
 * Make sure the caller does not hold these two locks. */
static void queue_deferred_io_job(struct io_job *io)
{
        //unsigned long flags;
        struct deferred_stats *def_stats;

        //spin_lock_irqsave(&jobs_lock, flags);
        spin_lock_bh(&jobs_lock);
        def_stats = this_cpu_ptr(&deferred_stats);
        __add_deferred_io_job(io);
        ++def_stats->total;
        //spin_unlock_irqrestore(&jobs_lock, flags);
        spin_unlock_bh(&jobs_lock);

        wake_deferred_wqueue();
}

/* The only entry point into the gc; can be called from interrupt context */
/*
static void wake_gc(struct io_job *io)
{
        set_io_job_gc(io);
        io->page = NULL;
        queue_deferred_io_job(io);
}*/

/* Runs on the global workqueue, serialized w/ the IO completion
 * work_structs since the workqueue is singlethreaded. */
 /*
  * (zgy): the io_job the auther allocates here is empty because the original
  * design always do GC from the tail. We do not need to know which block need
  * to be re-placed.
  */
/*
static void try_sched_gc(struct work_struct *work)
{
        struct dm_gecko *dmg = container_of(work, struct dm_gecko, gc_work);
	struct io_job *io;
	int i;

	// Optimistic estimate of the # of gc requests that can be
	// issued --- read the dmg->gc_req_in_progress without holding
	// the dmg->lock.
	int gc_requests = (dmg->max_gc_req_in_progress -
			   dmg->gc_req_in_progress);
        // ziggy DEBUG
        //printk("ziggy: %s, gc_req_in_progress = %d, gc_requests = %d\n", __FUNCTION__, dmg->gc_req_in_progress, gc_requests);


	if (gc_requests < 1) {
	  gc_requests = 1;
	}

	for (i = 0; i < gc_requests; ++i) {
	  atomic_inc(&dmg->total_jobs);
	  io = mempool_alloc(io_job_mempool, GFP_NOIO);

          io->dmg = dmg;
	  wake_gc(io);
	}
        atomic_set(&dmg->gc_work_scheduled_by_timer, 0);
}*/

/* this executes in irq context; can't mempool_alloc w/ the GFP_NOIO
 * flag */
/*
static enum hrtimer_restart fire_gc_timer(struct hrtimer *timer)
{
        struct dm_gecko *dmg = container_of(timer, struct dm_gecko, timer);

        if (!atomic_read(&dmg->timer_active)) {
                return HRTIMER_NORESTART;
        }
        if (atomic_cmpxchg(&dmg->gc_work_scheduled_by_timer, 0, 1) == 0) {
                queue_work(gecko_wqueue, &dmg->gc_work);  // ziggy: Do GC
        }
        hrtimer_forward_now(timer, dmg->timer_delay);
        return HRTIMER_RESTART;
}*/

// TODO(tudorm): make this work for raid1 with # of stripes > 2
// and DM_GECKO_INDEPENDENT_GC / DM_GECKO_READ_TPUT;
static int default_read_stripe_for_layout(enum dm_gecko_layout layout) {
  int stripe = 0;
  switch(layout) {
  case linear:
  case raid0:
  case raid1:
          stripe = 0;
          break;
  case raid5:
  case raid6:
  default:
          printk(DM_GECKO_PREFIX "unimplemented layout\n");
          BUG_ON(1);
          break;
  }
  return stripe;
}

static int default_gc_stripe_for_layout(enum dm_gecko_layout layout) {
  int stripe = 0;
  switch(layout) {
  case linear:
  case raid0:
          stripe = 0;
          break;
  case raid1:
          stripe = 1;
          break;
  case raid5:
  case raid6:
  default:
          printk(DM_GECKO_PREFIX "unimplemented layout\n");
          BUG_ON(1);
          break;
  }
  return stripe;
}

static inline int choose_load_balanced_stripe(struct dm_gecko *dmg)
{
        /* load balance using the per-CPU counter for READs =>
         * sloppy counter */
        unsigned long long sloppy_read_cnt;
        get_cpu();
        /* can't just use reads, must also use the gc events,
         * otherwise, after a period of inactivity, when only
         * the gc runs, the sloppy_read_cnt remains the same,
         * thus all gc read requests will hit the same disk */
        sloppy_read_cnt = (this_cpu_ptr(dmg->stats))->reads +
                this_cpu_ptr(dmg->stats)->gc;
        put_cpu();
        return do_div(sloppy_read_cnt, dmg->disk_map.stripes);
}

static int choose_read_stripe(sector_t sector, struct dm_gecko *dmg)
{
        int stripe = default_read_stripe_for_layout(dmg->disk_map.layout);
        if (sector_in_seg(sector, dmg->head_seg)) {
                return choose_load_balanced_stripe(dmg);
        } else if (sector_in_seg(sector, dmg->tail_seg) &&
                   test_bit(DM_GECKO_INDEPENDENT_GC, &dmg->flags)) {
                return stripe;
        }
        // fall through
        if (test_bit(DM_GECKO_READ_TPUT, &dmg->flags)) {
                return choose_load_balanced_stripe(dmg);
        } else {
                return stripe;
        }
}

static int choose_gc_stripe(sector_t sector, struct dm_gecko *dmg)
{
        if (sector_in_seg(sector, dmg->head_seg)) {
                return choose_load_balanced_stripe(dmg);
        } else if (test_bit(DM_GECKO_INDEPENDENT_GC, &dmg->flags)) {
                // TODO(tudorm): BUG_ON if not on the tail segment.
                return default_gc_stripe_for_layout(dmg->disk_map.layout);
        } else {
                if (test_bit(DM_GECKO_READ_TPUT, &dmg->flags)) {
                        return choose_load_balanced_stripe(dmg);
                } else {
                        // yes, READ stripe!
                        return default_read_stripe_for_layout(
                                dmg->disk_map.layout);
                }
        }
}

// straced hdparm and used the following symbols from its source
#define SG_ATA_16             0x85
#define SG_ATA_16_LEN         16
#define ATA_USING_LBA         (1 << 6)
#define ATA_OP_SLEEPNOW1      0xe6
#define ATA_OP_SLEEPNOW2      0x99
#define ATA_OP_STANDBYNOW1    0xe0
#define ATA_OP_STANDBYNOW2    0x94
#define ATA_OP_SETIDLE        0xe3
#define SG_ATA_PROTO_NON_DATA (3 << 1)
#define SG_CDB2_CHECK_COND    (1 << 5)

static void prep_SG_ATA_cmd_block(unsigned char *cmd_block,
                                  enum seg_power_state pow_state)
{
        BUG_ON(pow_state == unspecified);
        cmd_block[0] = SG_ATA_16;
        cmd_block[1] = SG_ATA_PROTO_NON_DATA;
        cmd_block[2] = SG_CDB2_CHECK_COND;
        cmd_block[13] = ATA_USING_LBA;
        switch (pow_state) {
        case active:
                cmd_block[6] = 0;        // set the delay to 0
                cmd_block[14] = ATA_OP_SETIDLE;
                break;
        case standby:
                cmd_block[14] = ATA_OP_STANDBYNOW1;
                break;
        case sleep:
                cmd_block[14] = ATA_OP_SLEEPNOW1;
                break;
        default:
                BUG_ON(1);
        }
}

/* Put device into active, standby, or sleep mode. If the device is
 * put into lowest power sleep mode, it will be shut down
 * completely. A reset is required before the drive can be accessed
 * again, and the Linux IDE driver should automatically issue the
 * reset on demand (tested on a 2.6.35 kernel and it does indeed
 * automatically issue the reset). */
static void set_drive_power(struct block_device *bdev,
                            enum seg_power_state pow_state)
{
        mm_segment_t old_fs = get_fs();
        struct gendisk *disk = bdev->bd_disk;
        struct sg_io_hdr hdr;
        unsigned char sense_b[32];
        unsigned char cmd_block[SG_ATA_16_LEN];
        int err;

        memset(&hdr, 0, sizeof(hdr));
        memset(&sense_b, 0, sizeof(sense_b));
        memset(cmd_block, 0, sizeof(cmd_block));
        prep_SG_ATA_cmd_block((unsigned char *)&cmd_block, pow_state);

        hdr.interface_id = SG_INTERFACE_ID_ORIG;
        hdr.dxfer_direction = SG_DXFER_NONE;
        hdr.cmd_len = sizeof(cmd_block);
        hdr.mx_sb_len = sizeof(sense_b);
        hdr.sbp = sense_b;
        hdr.cmdp = cmd_block;
        hdr.timeout = 10000;        // timeout in milliseconds

        set_fs(KERNEL_DS);
        err = blkdev_ioctl(bdev, 0, SG_IO, (unsigned long)&hdr);
        if (err) {
                printk(DM_GECKO_PREFIX "sg_io error %d on %s\n", err,
                       disk->disk_name);
        } else {
                printk(DM_GECKO_PREFIX
                       "set /dev/%s drive power state to %s\n",
                       disk->disk_name,
                       pow_state ==
                       active ? "active" : ((pow_state == standby) ?
                                            "standby" : "sleep"));
        }
        set_fs(old_fs);
}
 
/* ziggy: This function is for change power state of device(s) of a segment.
 *
 * Here @stripe is not the stripe in RAID, instead they are real devices within
 * a segment.
 * For example, one RAIN 1 with strip 1 means each segment has two devices.
 */
static void run_dm_dev_seg(struct work_struct *work)
{
        struct dm_dev_seg *seg = container_of(work, struct dm_dev_seg, work);
        struct dm_gecko *dmg = seg->ctxt;

        if (dmg->disk_map.layout != raid1 ||
            seg->next_pow_state == seg->cur_pow_state ||
            test_bit(DM_GECKO_READ_TPUT, &dmg->flags) ||
            seg == dmg->head_seg ||
            (seg == dmg->tail_seg &&
            test_bit(DM_GECKO_INDEPENDENT_GC, &dmg->flags))) {
                goto out_reset_next_pow_state;
        }

        if (seg->next_pow_state == standby || seg->next_pow_state == sleep) {
                int i, err;
                for (i = 0; i < dmg->disk_map.stripes; i++) {
                /* blocking flush while on workqueue's task context,
                 * hence will block deferred IO or gc events scheduled
                 * on same workqueue. This flush is not necessary
                 * if this was called as a result of tail advancing. */
                        err = blkdev_issue_flush(seg->dev[i]->bdev, GFP_KERNEL,
#if LINUX_VERSION_CODE <= KERNEL_VERSION(2, 6, 36)
                                                 NULL, BLKDEV_IFL_WAIT);
#else
                                                 NULL);
#endif
                        if (err) {
                                printk(DM_GECKO_PREFIX "disk flush failed "
                                       "with status %d\n", err);
                        }
                        if (i != default_read_stripe_for_layout(
                                dmg->disk_map.layout)) {
                                set_drive_power(seg->dev[i]->bdev,
                                                seg->next_pow_state);
                        }
                }
                seg->cur_pow_state = seg->next_pow_state;
        }
out_reset_next_pow_state:
        seg->next_pow_state = unspecified;
}

static int store_dm_gecko(struct dm_gecko *dmg);

static void do_sync_metadata(struct dm_gecko *dmg)
{
        unsigned long saved_flags;
        int err = 0;

        BUG_ON(in_interrupt());

	// Optimization that does not allow two (non-final)
	// metadata-sync operations to proceed at roughly the same
	// time.
        if (test_and_set_bit(DM_GECKO_SYNCING_METADATA, &dmg->flags)) {
                printk(DM_GECKO_PREFIX "A metadata-sync operation is already "
                       "in progress.\n");
		return;
	}

        down_write(&dmg->metadata_sync_sema);
	saved_flags = dmg->flags;
        // No more new IOs are being submitted from this point on.
        if (test_bit(DM_GECKO_FINAL_SYNC_METADATA, &dmg->flags)) {
                printk(DM_GECKO_PREFIX "Should not be able to issue a "
                       "metadata-sync operation after target destroy.\n");
                BUG_ON(true);
        }
        // Turn off the gc.
        set_bit(DM_GECKO_GC_FORCE_STOP, &dmg->flags);
        // Turn off the (gc) timer.
        atomic_set(&dmg->timer_active, 0);
        hrtimer_cancel(&dmg->timer);
        // Wait for all pending io jobs (including gc jobs) to finish.
        wait_event(dmg->jobs_pending_waitqueue, !atomic_read(&dmg->total_jobs));

        err = store_dm_gecko(dmg);
        if (err) {
                printk(DM_GECKO_PREFIX "Unable to store gecko metadata\n");
                goto out;
        }
        dmg->persistent_tail = dmg->tail;
        dmg->persistent_available_blocks = atomic_read(&dmg->available_blocks);

out:
        dmg->flags = saved_flags;  // Restore other flags.
	clear_bit(DM_GECKO_SYNCING_METADATA, &dmg->flags);
        up_write(&dmg->metadata_sync_sema);
}

/*
static void sync_metadata(struct work_struct *work)
{
        struct dm_gecko *dmg = container_of(work,  // pointer to the member
                                            struct dm_gecko,  // container
                                            sync_metadata_work);  //name of the member within the struct
        do_sync_metadata(dmg);
}*/

static void run_deferred_jobs(struct work_struct *unused_work_struct)
{
        //unsigned long flags;
        struct deferred_stats *def_stats;

        BUG_ON(in_interrupt());

        //spin_lock_irqsave(&jobs_lock, flags);
        spin_lock_bh(&jobs_lock);
        /* preemption disabled under spinlock */
        def_stats = this_cpu_ptr(&deferred_stats);
        while (!list_empty(&deferred_jobs)) {
                struct io_job *io =
                    container_of(deferred_jobs.next, struct io_job, list);
                list_del(&io->list);
                --def_stats->total;

                if (io_job_is_gc(io)) {
                        BUG_ON(1);
                        ++def_stats->gc;
                        //spin_unlock_irqrestore(&jobs_lock, flags);
                        spin_unlock_bh(&jobs_lock);
                        BUG_ON(!io_job_is_deferred(io));
                        //do_run_gc(io);
                } else {
                        ++def_stats->rw;
                        //spin_unlock_irqrestore(&jobs_lock, flags);
                        spin_unlock_bh(&jobs_lock);
                        BUG_ON(!io_job_is_deferred(io));
                        map_rw_io_job(io);
                }
                //spin_lock_irqsave(&jobs_lock, flags);
                spin_lock_bh(&jobs_lock);
		// May have migrated CPUs so grab a fresh reference.
		def_stats = this_cpu_ptr(&deferred_stats);
        }
        //spin_unlock_irqrestore(&jobs_lock, flags);
        spin_unlock_bh(&jobs_lock);
}

/* operation on hash table */
static struct io_for_block *get_io_for_block(struct dm_gecko *dmg,
                                             sector_t key)
{
        struct io_for_block *io4b;

        unsigned long bucket_idx = hash_long(key, HASH_TABLE_BITS);
        struct list_head *bucket = &dmg->buckets[bucket_idx];

        list_for_each_entry(io4b, bucket, hashtable) {
                if (io4b->key == key) {
                        return io4b;
                }
        }
        return NULL;
}

/* WARNING: duplicates are not checked for, you have been advised,
 * play nice */
static void put_io_for_block(struct dm_gecko *dmg, u32 key,
                             struct io_for_block *io4b)
{
        // ziggy: track io for the same v_block together
        unsigned long bucket_idx = hash_long(key, HASH_TABLE_BITS);
        struct list_head *bucket = &dmg->buckets[bucket_idx];

        io4b->key = key;
        list_add_tail(&io4b->hashtable, bucket);
        //++dmg->htable_size;
        atomic_inc(&dmg->htable_size);
}

static spinlock_t *get_lock_for_table(struct dm_gecko *dmg, sector_t key)
{
        unsigned long bucket_idx = hash_long(key, HASH_TABLE_BITS);
        return &dmg->htable_entry_locks[bucket_idx];
}

static void wake_up_free_space_available(struct dm_gecko *dmg)
{
        //unsigned long flags;
        //spin_lock_irqsave(&dmg->lock, flags);
        spin_lock_bh(&dmg->lock);
        //__wake_up_locked(&dmg->no_free_space_waitqueue, TASK_NORMAL);
        // ziggy - TODO: should I only wake the first waitqueue or wake them
        // all? The original one only wake the first waitqueue in the head list
        // ANSWER: the original design only wakeup one task because it is single-threaded.
        // This function is called everytime a block relocation finished. This is okay.
        __wake_up_locked(&dmg->no_free_space_waitqueue, TASK_NORMAL, 1);
        //spin_unlock_irqrestore(&dmg->lock, flags);
        spin_unlock_bh(&dmg->lock);
}

static inline u32 __relocatable_blocks(struct dm_gecko *dmg)
{
        return atomic_read(&dmg->free_blocks) - atomic_read(&dmg->available_blocks);
}

static inline u32 __unavailable_blocks(struct dm_gecko *dmg)
{
        return dmg->size - atomic_read(&dmg->available_blocks);
}

static inline u32 __used_blocks(struct dm_gecko *dmg)
{
        return dmg->size - atomic_read(&dmg->free_blocks);
}

/* Should probably encode this in a proper DFA */
/* ->lock must be held */
#if 0
static int __gc_needs_to_run(struct dm_gecko *dmg)
{
        /* TODO: check how many available and free blocks there are,
         * and their ratio use a bunch of watermarks, e.g. when <= 10%
         * contiguous available space, and start the gc. When the
         * reserved block percentage is hit (say 5%) then block the
         * writers. Make sure the gc can continue to make progress */

        //u32 used_blocks = __used_blocks(dmg);
        //u32 unavailable_blocks = __unavailable_blocks(dmg);
        u32 max_relocatable_blocks = __relocatable_blocks(dmg);

#ifdef ALWAYS_RUN_GC
        return (max_relocatable_blocks != 0);
#endif

	if (dmg->gc_req_in_progress >= dmg->max_gc_req_in_progress) {
                return 0;
	}

        if (test_bit(DM_GECKO_GC_FORCE_STOP, &dmg->flags)) {
                return 0;
        }
        /* Return `no need to run gc' early if:
         *      - there are no gaps / holes
         *      - there are no more concurrent gc requests allowed */
        if (max_relocatable_blocks == 0) {
                clear_bit(DM_GECKO_GC_STARTED, &dmg->flags);
                return 0;
        }

        if (test_bit(DM_GECKO_GC_STARTED, &dmg->flags)) {
                if (max_relocatable_blocks <= dmg->gc_ctrl.low_watermark) {
                        clear_bit(DM_GECKO_GC_STARTED, &dmg->flags);
                        return 0;
                } else {
                        return 1;
                }
        } else {
                if (max_relocatable_blocks >= dmg->gc_ctrl.high_watermark) {
                        set_bit(DM_GECKO_GC_STARTED, &dmg->flags);
                        return 1;
                } else {
                        return 0;
                }
        }
}
#endif

// the function may be called from irq context, so can't block
static void sched_delayed_power_adjustment_for_segment(struct dm_dev_seg
                                                       *seg,
                                                       enum seg_power_state
                                                       next_pow_state)
{
        struct dm_gecko *dmg = seg->ctxt;
        if (seg == dmg->head_seg || dmg->disk_map.layout != raid1 ||
            (seg == dmg->tail_seg &&
            test_bit(DM_GECKO_INDEPENDENT_GC, &dmg->flags))) {
                return;
        }
        seg->next_pow_state = next_pow_state;
        queue_work(gecko_wqueue, &seg->work);
}

/*
 * ziggy: Update the d_map here.
 * After GC, it map the new linear block number to the v_block.
 */
static void __relocate_gc_written_block(struct io_job *io)
{
        struct dm_gecko *dmg = io->dmg;
        u32 old_l_block = dmg->d_map[io->v_block];
        u32 v_blk;
        u32 v_blk_old;

        v_blk_old = get_rmap_element(dmg, old_l_block);
        v_blk = get_rmap_element(dmg, io->l_block);

        BUG_ON(is_block_marked_free(v_blk_old, dmg));  // happens when pending WRITE overwriting the moved valid block
        BUG_ON(v_blk_old != io->v_block);
        BUG_ON(v_blk != io->v_block);

        dmg->d_map[io->v_block] = io->l_block;
        update_rmap(dmg, old_l_block, dmg->size);
}

static void update_invalid_blk_nr(struct dm_gecko *dmg, u32 old_l_blk)
{
        sector_t sec;
        struct dm_dev_seg *dev;
        unsigned int seg_idx;
        int zone_idx;

        sec = block_to_sector(old_l_blk);
        dev = sector_to_dev(dmg, sec);
        seg_idx = sector_to_segment(sec - dev->start);
        atomic_inc(&dev->sit[seg_idx]->nr_invalid_blk);

        zone_idx = seg_idx >> SEG_TO_ZONE_SHIFT;
        atomic_inc(&dev->zones[zone_idx].nr_invalid_blk);
}

/* ->lock must be held */
/* zgy: there is only single request to a specific v_block is processing at a time.
 * Which means we do not need to hold any lock for this.*/
static void __relocate_written_block(struct io_job *io)  // ziggy: Is called after IO completion
{
        struct dm_gecko *dmg = io->dmg;
        u32 old_l_block;

        BUG_ON(get_rmap_element(dmg, io->l_block) != io->v_block);
        // Since the lock is held while calling this function, the
        // block indexed by io->v_block may be in one of two states
        // only. It may either be marked free, or it may point to a
        // linear block in the reverse map that correctly holds the
        // back link, irrespective of the fact that reads and writes
        // are issued concurrently.
        old_l_block = dmg->d_map[io->v_block];
        if (!is_block_marked_free(old_l_block, dmg)) {
                BUG_ON(get_rmap_element(dmg, old_l_block) != io->v_block);
                //dmg->r_map[old_l_block] = mark_block_free(dmg);
                update_rmap(dmg, old_l_block, dmg->size);  // mark as free
                /* ziggy: update the # of invalid block */
                update_invalid_blk_nr(dmg, old_l_block);  // move this part to block allocation procedure

                //++dmg->free_blocks;  // in our segment management approach, free_blocks and available_blocks share the same meaning
        }
        /* update the d_map. All map modifications are finished. */
        dmg->d_map[io->v_block] = io->l_block;
}

/* lock must be held */
//static struct dm_dev_seg *__touch_new_head_seg(struct dm_gecko *dmg, u32 l_blk)
static struct dm_dev_seg *__touch_new_head_seg(struct dm_gecko *dmg, struct dm_dev_seg *dev)
{
        struct dm_dev_seg *ret = NULL;
        struct dm_dev_seg *head_seg;
        //struct dm_dev_seg *dev = sector_to_dev(dmg, block_to_sector(l_blk));
        int is_last_write = atomic_dec_and_test(&dev->pending_writes);
        /* GC is waiting wrtes to finished.
         * Wake GC thread up if there is any. */
        if (is_last_write) {
            //__wake_up_locked(&dmg->pending_write_waitqueue, TASK_NORMAL, 1);  // zgy: help gc to start after the write on the victim block is finished.
            wake_up(&dmg->pending_write_waitqueue);
        }

        // ziggy: check if the lastest finished l_block is belongs to a new device
        if (dev != dmg->head_seg && is_last_write) {
                head_seg = dmg->head_seg;
                BUG_ON(dev->access_seq_in_log == head_seg->access_seq_in_log);
                if (dev->access_seq_in_log < head_seg->access_seq_in_log) {
                        dev->access_seq_in_log =
                            head_seg->access_seq_in_log + 1;
                        dmg->head_seg = dev;
                        ret = head_seg;
                }
        }

        return ret;
}

/*
** Check if there are deferred requests on the v_block.
** Resend them by adding them into the deferred_list.
** Also free the corresponding io4b object from the htable.
 */
static void __gc_postprocess_io4b(struct dm_gecko *dmg, u32 v_blk)
{
        //unsigned long table_flags;
        //unsigned long io4b_flags;
        //unsigned long flags_job;
        struct io_for_block *io4b;
        spinlock_t *htable_lock;
        int no_pendings = 0;

        /* There no other thread will free the io4b object because following requests
         * are deferred and wait for this GC thread to resend them.
         * Thus, in the GC complete callback, we can simply use spin_lock_bh without refetch
         * io4b from htable. */
        htable_lock = get_lock_for_table(dmg, v_blk);
        //spin_lock_irqsave(htable_lock, table_flags);
        spin_lock_bh(htable_lock);
        io4b = get_io_for_block(dmg, v_blk);
        BUG_ON(!io4b);  // the GC process added a io4b for this v_blk previously

        //spin_lock_irqsave(&io4b->lock, io4b_flags); // must lock it here because we want to access the io4b
        spin_lock_bh(&io4b->lock); // must lock it here because we want to access the io4b
        /* We can release htable_lock here after we have fetched io4b->lock
         * because there is no normal I/O threads can modify the io4v yet,
         * which means there is possible the io4b object will be freed. */
        //spin_unlock_irqrestore(htable_lock, table_flags);
        spin_unlock_bh(htable_lock);
        /* for modified gc identifier */
        BUG_ON(io4b->rw_cnt >= 0 && io4b->gc_while_pending == 0);

        /* the first io4b in deferred_io is gc */
        if (io4b->gc_while_pending) {
                io4b->gc_while_pending = 0;
        }

        /* whether there are pending reqs when send gc or not, there can be request in deferred_io */
        if (!list_empty(&io4b->deferred_io)) {  // ziggy: if there is any request to the same v_block
                struct io_job *rw_io_job, *tmp;
                //struct deferred_stats *def_stats;

        // profiling
#if ZGY_PROFILING
        struct timespec64 my_clock2[2];
        ktime_get_ts64(&my_clock2[0]);
#endif
        //
                //spin_lock_irqsave(&jobs_lock, flags_job);
                spin_lock_bh(&jobs_lock);
                //def_stats = this_cpu_ptr(&deferred_stats);
                list_for_each_entry_safe(rw_io_job, tmp,
                                         &io4b->deferred_io, list) {  // ziggy: add there io to global deferred_io queue
                        list_del(&rw_io_job->list);
                        __add_deferred_io_job(rw_io_job);
                        //++def_stats->total;
                }
                //spin_unlock_irqrestore(&jobs_lock, flags_job);
                spin_unlock_bh(&jobs_lock);

        // profiling
#if ZGY_PROFILING
        ktime_get_ts64(&my_clock2[1]);
        calclock(my_clock2, &gc_complete_joblock_time, &gc_complete_joblock_cnt);
#endif
        }
        if (io4b->rw_cnt <= 0) {  /* there is no pending reqs */
                //spin_lock_irqsave(htable_lock, table_flags);
                spin_lock_bh(htable_lock);
                list_del(&io4b->hashtable);  /* Deleted from hashtable */
                //spin_unlock_irqrestore(htable_lock, table_flags);
                spin_unlock_bh(htable_lock);

                atomic_dec(&dmg->htable_size);
                no_pendings = 1;
        }
        //spin_unlock_irqrestore(&io4b->lock, io4b_flags);  // I think it's okay to hold this lock for a long time
                                                          // since it only affects one single v_blk
        spin_unlock_bh(&io4b->lock);  // I think it's okay to hold this lock for a long time
        if (no_pendings) {
                mempool_free(io4b, io_for_block_mempool);
        }
}

/* zgy: kcopy client thread call this function */
static void seg_gc_complete_callback(int read_err, unsigned long write_err,
                                 void *ctxt)
{
        struct io_job *io = (struct io_job*)ctxt;
        //struct io_for_block *io4b;
        /* hold the reference, io_job may be released early */
        struct dm_gecko *dmg = io->dmg;
        //unsigned long flags_job;
        //unsigned long table_flags;
        //unsigned long io4b_flags;
        struct dm_dev_seg *src_dev;
        struct dm_dev_seg *dst_dev;
        u32 old_l_blk;
        sector_t sec;
        unsigned int seg_idx = io->seg_idx;
        int nr_freed_blks = BLOCKS_PER_SEGMENT - io->nr_valid_blks;
        //int no_pendings = 0;  /* if there is no pending IOs, we can free this io4b */

        /* TODO: if kcopyd fails, handle the errors as in the IO
         * completion */
        BUG_ON(read_err || write_err);

        old_l_blk = dmg->d_map[io->v_block];
        sec = block_to_sector(old_l_blk);
        src_dev = sector_to_dev(dmg, sec);

        __relocate_gc_written_block(io);

        /* Add free segment back to free list of dm_dev_seg. */
        if (seg_idx != -1) { /* last valid block in the segment */
                /* Reset sit entry & add it back to free_list*/
                atomic_add(nr_freed_blks, &dmg->available_blocks);
                atomic_add(nr_freed_blks, &dmg->free_blocks);
                atomic_set(&src_dev->sit[seg_idx]->nr_invalid_blk, 0);
                src_dev->sit[seg_idx]->itr = 0;
                /* Unlike in map_rw_io_job(), here, we do not need to hold lock.
                 * This function is executed by kcopyd client, and we only have
                 * one kcopyd client. */
                if (IS_FIRST_SEG) {  /* Need to assign new free seg for this cleaned device.*/
                        src_dev->curr_seg = src_dev->sit[seg_idx];
                        IS_FIRST_SEG = not;
                } else {
                        list_add_tail(&(src_dev->sit[seg_idx]->list), &src_dev->free_list.list);
                        src_dev->free_seg_nr++;
                }

                if (io->reclaim_zone_idx != -1) {
                        atomic_set(&src_dev->zones[io->reclaim_zone_idx].nr_invalid_blk, 0);
                }
        }

        __gc_postprocess_io4b(dmg, io->v_block);

        dst_dev = io->dest_dev;
        /*  TODO: remove tracking write on read only disk
        if (atomic_dec_and_test(&dst_dev->pending_writes)) {  // return true if pending_write equals 0
                wake_up(&dmg->pending_write_waitqueue);
        } */
        mempool_free(io, io_job_mempool);
        do_complete_generic(dmg);
        //atomic_dec(&dmg->total_jobs);  // do_complete_generic has already done this

        //atomic_dec(&dmg->gc_wait_for_req);

        atomic_dec(&dmg->nr_kcopyd_jobs);
        wake_up(&dmg->kcopyd_job_waitqueue);
        //wake_up_free_space_available(dmg);
        //__wake_up_locked(&dmg->no_free_space_waitqueue, TASK_NORMAL, 1);
        wake_up(&dmg->no_free_space_waitqueue);
        wake_deferred_wqueue();

        //debug
        //printk("%s: metadate updating has finished. Single valid blk GC finished here\n", __FUNCTION__);
        // profiling
#if ZGY_PROFILING
        ktime_get_ts64(&gc_clock[1]);
        calclock2(gc_clock, &gc_running_time, &gc_running_count);
#endif
}

/* zgy: kcopy client thread call this function */
static void seg_gc_whole_seg_complete_callback(int read_err, unsigned long write_err,
                                 void *ctxt)
{
        struct io_job *io = (struct io_job *)ctxt;
        //struct io_for_block *io4b = NULL;
        struct dm_gecko *dmg = io->dmg;
        //unsigned long flags_job;
        //unsigned long table_flags;
        //unsigned long io4b_flags;
        struct dm_dev_seg *victim_dev;
        sector_t old_l_blk;
        sector_t v_blk;
        unsigned int freed_blk_nr;
        //int no_pendings;

        //struct io_job *rw_io_job, *tmp;
        //struct deferred_stats *def_stats;

        int i;

        //debug
        //static int count = 0;
        ZDBG("zgy: %s, %d GC write has finished !!! Working on metadata updating\n", __FUNCTION__, ++count);

        /* TODO: if kcopyd fails, handle the errors as in the IO
         * completion */
        BUG_ON(read_err || write_err);

        if (io->nr_valid_blks == 0) {
                goto fast_reclaim;  // there is no valid block in the victim segment
        }

        victim_dev = sector_to_dev(dmg, block_to_sector(io->l_block));
        freed_blk_nr = BLOCKS_PER_SEGMENT - io->nr_valid_blks;

        /* Reset sit entry & add it back to free_list*/
        atomic_add(freed_blk_nr, &dmg->available_blocks);
        atomic_add(freed_blk_nr, &dmg->free_blocks);
        atomic_set(&victim_dev->sit[io->seg_idx]->nr_invalid_blk, 0);
        victim_dev->sit[io->seg_idx]->itr = 0;  // since the segment was full, only this GC job will change this value in this moment.

        /* update mapping table */
        for (i = 0; i < io->nr_valid_blks; i++) {
                v_blk = io->blks_info[i].v_blk;
                old_l_blk = dmg->d_map[v_blk];
                dmg->d_map[v_blk] = io->blks_info[i].l_blk;
                update_rmap(dmg, old_l_blk, dmg->size);
        }

        /* Do not need lock here because there is one kcopyd thread working on this */
        if (IS_FIRST_SEG) {  /* Need to assign new free seg for this cleaned device.*/
                victim_dev->curr_seg = victim_dev->sit[io->seg_idx];
                IS_FIRST_SEG = not;  // reset to is at the GC entry point (when swap disks)
        } else {
                list_add_tail(&(victim_dev->sit[io->seg_idx]->list), &victim_dev->free_list.list);
                victim_dev->free_seg_nr++;
                //dmg->total_free_seg_nr++;
        }

        /* do follwing work for every valid blocks in the segment */
        for (i = 0; i < io->nr_valid_blks; i++) {
                v_blk = io->blks_info[i].v_blk;
                __gc_postprocess_io4b(dmg, v_blk);
        }
        kfree(io->blks_info);  // allocate in map_dst_for_seg_gc after reading from disk

fast_reclaim:
        if (io->reclaim_zone_idx != -1) {
                atomic_set(&victim_dev->zones[io->reclaim_zone_idx].nr_invalid_blk, 0);
        }

        // we check pending GC only, need_move2() can handle it
        //atomic_dec(&dmg->gc_wait_for_req);

        //__wake_up_locked(&dmg->no_free_space_waitqueue, TASK_NORMAL, 1);  // when rw waiting for free blocks
        wake_up(&dmg->no_free_space_waitqueue);  // when rw waiting for free blocks
        wake_deferred_wqueue();  // this thread also fetch jobs_lock


        /* ziggy: updating pending_writes which helps us to know when to start GC*/
        //struct dm_dev_seg *seg = io->dest_dev;
        // May be a optional operation since GC are waiting for writing on swapped out device,
        // however, the "dest_dev" is not the GC victim device
        /*
        if (atomic_dec_and_test(&(io->dest_dev->pending_writes))) {
                wake_up(&dmg->pending_write_waitqueue);
        }*/

        mempool_free(io, io_job_mempool);
        do_complete_generic(dmg);  // also decrease total_jobs

        atomic_dec(&dmg->nr_kcopyd_jobs); /* to limit # of concurrent kcopy jobs */
        wake_up(&dmg->kcopyd_job_waitqueue);

        //debug
        ZDBG("zgy: %s, %d GC write has finished. GC of a segment is finished !!! \n", __FUNCTION__, count);
        //printk("zgy: %s, %d GC write has finished. GC of a segment is finished !!! \n", __FUNCTION__, count++);
}

/* Cannot be called from interrupt context */
/*  zgy: this is called by kcopyd thread */
void seg_gc_complete_read_noirq(int *dst_count,
                            struct dm_io_region *dst,
                            void *context) {
        struct io_job *io = (struct io_job *) context;
        struct dm_gecko *dmg = io->dmg;
        struct dm_dev_seg* dst_dev;

        //BUG_ON(in_interrupt());
        BUG_ON(*dst_count > 0);
        BUG_ON(is_block_free_or_invalid(io->l_block, dmg)); /* Cannot be free, it is the destination l_block */

        /* Since the l_block is unique, no lock is needed. */
        /* Update the mapping entry for l_block.
         * The d_map for v_block has not been updated yet.*/
        update_rmap(dmg, io->l_block, io->v_block);

        // zgy: set write info to ~dst~
        dst_dev = linear_to_phy_all(dmg, block_to_sector(io->l_block), dst,
                                dst_count);
        // TODO: GC preemption
        // wait if there are ongoing read on the target device.
        // there is no ongoing writes because it is body disk
        wait_event(dmg->gc_pending_read_waitqueue, !atomic_read(&dst_dev->pending_reads));

        // we have mapped the destination, now writing starts
        // TODO: do we need track pending writes on destination disk?
        // I think this is a bad ideal because there is no other writes on them after GC starts
        //atomic_inc(&dst_dev->pending_writes);
}

/* need_move - predicate if the block need to be moved
 * @src_blk: a valid block in the victim segment
 * @dmg: gecko target metadata
 *
 * Description:
 *   Check if there are pending write requests on the corresponding v_blk of src_blk.
 *   If there is no pending writes, we update corresponding io_for_block rw_cnt and gc_while_pending
 *   to block following normal IO into deferred_io list.
 */
static inline int need_move(struct dm_gecko *dmg, u32 src_blk, u32 v_blk)
{
        /* zgy: make sure there is no pending writes on the v_blk before we send the req */
        //struct dm_gecko_stats *stats;
        struct io_for_block *extant_io4b, *io4b;
        //unsigned long table_flags;
        //unsigned long io4b_flags;
        spinlock_t *htable_lock;

        struct io_job *rw_io_job, *tmp;
        int dispatch_gc = 1;

        htable_lock = get_lock_for_table(dmg, v_blk);

refetch_io4b:
        //spin_lock_irqsave(htable_lock, table_flags);
        spin_lock_bh(htable_lock);
        extant_io4b = get_io_for_block(dmg, v_blk);
        if (!extant_io4b) {
                io4b = mempool_alloc(io_for_block_mempool, GFP_NOIO);
                io4b->rw_cnt = -1;  /* means this v_blk is under gc, so map_rw_io will put following requests to deferred_io list */
                io4b->gc_while_pending = 0;
                INIT_LIST_HEAD(&io4b->pending_io);
                INIT_LIST_HEAD(&io4b->deferred_io);
                spin_lock_init(&io4b->lock);

                put_io_for_block(dmg, v_blk, io4b);
                //spin_unlock_irqrestore(htable_lock, table_flags);
                spin_unlock_bh(htable_lock);
        } else {
                //spin_lock_irqsave(&extant_io4b->lock, io4b_flags);
                /* We cannot release the htable_lock before we fetch the io4b->lock
                 * because the io_complete_callback() may free or modify the io4b object.*/
                //if (!spin_trylock_irqsave(&extant_io4b->lock, io4b_flags)) {
                if (!spin_trylock(&extant_io4b->lock)) {
                        //spin_unlock_irqrestore(htable_lock, table_flags);
                        spin_unlock_bh(htable_lock);
                        goto refetch_io4b;
                }
                //spin_unlock_irqrestore(htable_lock, table_flags);
                spin_unlock_bh(htable_lock);

                /* preemption is disabled under spinlock */
                //stats = this_cpu_ptr(dmg->stats);
                //BUG_ON(list_empty(&extant_io4b->pending_io));
                //++stats->gc_rw_clash;

                /* Yes, the pending_io can be empty because io_complete() access hold the lock before
                 * this function holds the io4b->lock. */

                //if (!list_empty(&extant_io4b->pending_io)) {
                BUG_ON(list_empty(&extant_io4b->pending_io));
                list_for_each_entry_safe(rw_io_job, tmp,
                                        &extant_io4b->pending_io, list) {
                        if ((rw_io_job->rw == WRITE) ||
                        (rw_io_job->rw == READ && bio_data_dir(rw_io_job->bio) == WRITE)) { /* read modify write */
                                dispatch_gc = 0;
                                break;
                        }
                }
                //}
                /* Add io4b to block following reqs on the same v_blk */
                if (dispatch_gc) {
                        BUG_ON(list_empty(&extant_io4b->pending_io) && list_empty(&extant_io4b->deferred_io));
                        BUG_ON(extant_io4b->rw_cnt <= 0);
                        extant_io4b->gc_while_pending = 1;
                }

                //spin_unlock_irqrestore(&extant_io4b->lock, io4b_flags);
                spin_unlock_bh(&extant_io4b->lock);
        }

        ZDBG("%s, have checked if whether need to move this block. dispatch_gc = %d\n", __FUNCTION__, dispatch_gc);

        return dispatch_gc;
}

/*
** No gc_while_pending anymore.
** We do not start gc if there are pending IO on the v_blk
** Let's see if this fix the problem when using filesystem
 */
static inline int need_move2(struct dm_gecko *dmg, u32 src_blk, u32 v_blk)
{
        /* zgy: make sure there is no pending writes on the v_blk before we send the req */
        //struct dm_gecko_stats *stats;
        struct io_for_block *extant_io4b, *io4b;
        //unsigned long table_flags;
        spinlock_t *htable_lock;

        int dispatch_gc = 1;

        htable_lock = get_lock_for_table(dmg, v_blk);

refetch_io4b:
        //spin_lock_irqsave(htable_lock, table_flags);
        spin_lock_bh(htable_lock);
        extant_io4b = get_io_for_block(dmg, v_blk);
        if (!extant_io4b) {
                io4b = mempool_alloc(io_for_block_mempool, GFP_NOIO);
                io4b->rw_cnt = -1;  /* means this v_blk is under gc, so map_rw_io will put following requests to deferred_io list */
                io4b->gc_while_pending = 0;
                INIT_LIST_HEAD(&io4b->pending_io);
                INIT_LIST_HEAD(&io4b->deferred_io);
                spin_lock_init(&io4b->lock);

                put_io_for_block(dmg, v_blk, io4b);
                //spin_unlock_irqrestore(htable_lock, table_flags);
                spin_unlock_bh(htable_lock);
        } else {  // if there are extant_io4b, means there are ongoing reqs, we should go to sleep!
                //spin_unlock_irqrestore(htable_lock, table_flags); /* release cuz we are going to sleep */
                spin_unlock_bh(htable_lock);


                DECLARE_WAITQUEUE(__wait_pending_io, current);  // current is a macro that gives the task_structure of current process
                __add_wait_queue(&dmg->gc_waitqueue, &__wait_pending_io);

                atomic_inc(&dmg->gc_wait_for_req);  // tells io_complete that there are GC waiting for req completion

                //printk("zgy: in %s, GC is waiting for pending IO !!!!!\n", __FUNCTION__);
                __set_current_state(TASK_UNINTERRUPTIBLE);
                BUG_ON(in_interrupt());

                schedule();  // goto sleep

                __set_current_state(TASK_RUNNING);
                __remove_wait_queue(&dmg->gc_waitqueue, &__wait_pending_io);
                atomic_dec(&dmg->gc_wait_for_req);

                // after wakeup, recheck the rmap
                if (is_block_marked_free(get_rmap_element(dmg, src_blk), dmg)) {  // previous pending is write, no need to move it anymore
                        dispatch_gc = 0;
                } else { // reverse map stil valid (previous is READ)
                         // refetch the extant_io4b to make sure there is no pending io
                        goto refetch_io4b;
                }
        }

        ZDBG("%s, have checked if whether need to move this block. dispatch_gc = %d\n", __FUNCTION__, dispatch_gc);

        return dispatch_gc;
}

static inline void copy_valid_page(int valid, struct kcopyd_job *job, struct seg_gc_page_position_list *head) {
        struct page *x = NULL, *y = NULL;
        struct page_list *cursor;
        struct seg_gc_page_position_list *pos_node;
        int dst;
        int i;

        pos_node = list_first_entry(&head->list, struct seg_gc_page_position_list, list);
        list_del(head->list.next);  // delete the first node
        dst = pos_node->position;
        kfree(pos_node);

        // valid is always greater than position.
        for (i = 0, cursor = job->pages; i < valid; i++, cursor = cursor->next) {
                if (i == dst)
                        x = cursor->page;
        }
        y = cursor->page;

        // I think we do not need lock when we modify the page_list because the
        // list is belongs to a job and seems only one thread can hold a job at a time
        memcpy(page_address(x), page_address(y), GECKO_BLOCK_SIZE);
}

/* free_blks_for_gc - allcoate free blocks for moving valid blocks
 * @dev: destination device
 * @curr_blk: stores the begin blk of a region. Used to update r_map in caller
 *
 * @return: indicate if current block is from a new free segment. 0 == False
 *
 * Because we move a segment at a time. We only need at most two regions (segments) to write to.
 */
static inline int free_blks_for_gc(struct dm_dev_seg *dev, u32 *curr_blk)
{
        /* the caller hold the dmg-lock */
        struct dm_gecko *dmg = dev->ctxt;
        u32 blk;
        struct dev_sit_entry *free_seg;
        u32 base_blk;
        int new_seg = 0;

        //printk("%s, before allocate free block for GC\n", __FUNCTION__);

        blk = dev->curr_seg->seg_nr * BLOCKS_PER_SEGMENT + dev->curr_seg->itr;
        if (is_last_block_in_seg(dev->curr_seg->itr)) {  // allocate new segment or move to other dev
                if (dev->free_seg_nr) {
                        free_seg = list_first_entry(&(dev->free_list.list), struct dev_sit_entry, list);
                        list_del(&free_seg->list);
                        dev->curr_seg = free_seg;
                        dev->free_seg_nr--;  // the caller has already hold the lock
                        //dmg->total_free_seg_nr--;

                        new_seg = 1;
                } else {  // TODO This device is full, need to handle
                        printk("!!! In function %s, unfinished implementation. Destination disk full during GC !!!\n", __FUNCTION__);
                        BUG_ON(1);
                }
        } else {  // allocate next block in current segment
                dev->curr_seg->itr++;
        }

        atomic_dec(&dmg->available_blocks);
        atomic_dec(&dmg->free_blocks);

        base_blk = sector_to_block(dev->start);
        *curr_blk = blk + base_blk;

        //printk("%s, finished allocation of free block for GC\n", __FUNCTION__);
        return new_seg;
}

/* map_dst_for_seg_gc - allocate free blocks for valid blocks, also update r_map
 *
 * No lock required because there is only one single thread workqueue does the job */
static void map_dst_for_seg_gc(struct dm_gecko *dmg,
                               struct dm_dev_seg *dest_device,
                               struct dm_io_region *where,
                               unsigned int nr_blk,
                               struct valid_blks_info *valid_blks,
                               struct kcopyd_job *job)
{
        int j;
        u32 cursor = 0;
        int need_two_regions = 0;
        int nr_blk_1st_range = 0;
        sector_t sector_1, sector_2;

        ZDBG("%s start. following free block allocation\n", __FUNCTION__);

        /* normal IO and GC are using different devices */

        /* Do not put the following part into the for loop because there may be
         * two start sectors (two segments). */
        free_blks_for_gc(dest_device, &cursor);
        BUG_ON(!is_block_marked_free(get_rmap_element(dmg, cursor), dmg));
        sector_1 = block_to_sector(cursor);
        // TODO: can have wrong pointer value
        update_rmap(dmg, cursor, valid_blks[0].v_blk);
        valid_blks[0].l_blk = cursor;

        for (j = 1; j < nr_blk-1; j++) {
                if (free_blks_for_gc(dest_device, &cursor)) { // current blk is the last block of the segment
                        update_rmap(dmg, cursor, valid_blks[j].v_blk);
                        valid_blks[j].l_blk = cursor;
                        nr_blk_1st_range = ++j;

                        need_two_regions = 1;
                        free_blks_for_gc(dest_device, &cursor);
                        sector_2 = block_to_sector(cursor);  // must be placed after free_blks_for_gc because it allcoate a new block in a new free segment
                }
                update_rmap(dmg, cursor, valid_blks[j].v_blk);
                valid_blks[j].l_blk = cursor;
        }
        if (j == nr_blk - 1) {
                free_blks_for_gc(dest_device, &cursor);
                update_rmap(dmg, cursor, valid_blks[j].v_blk);
                valid_blks[j].l_blk = cursor;
        }

        // where has 8 slots. we only need to change job->num_dests
        where[0].bdev = dest_device->dev[0]->bdev;
        where[0].sector = sector_1 - dest_device->start;

        if (need_two_regions) {
                where[1].bdev = dest_device->dev[0]->bdev;
                where[1].sector = sector_2 - dest_device->start;
                where[1].count = GECKO_SECTORS_PER_BLOCK * (nr_blk - nr_blk_1st_range);
                where[0].count = GECKO_SECTORS_PER_BLOCK * nr_blk_1st_range;
                job->num_dests = 2;
        } else {
                where[0].count = GECKO_SECTORS_PER_BLOCK * nr_blk;
                job->num_dests = 1;
        }

        ZDBG("%s end.\n", __FUNCTION__);
}

static void move_to_the_end(struct kcopyd_job *job, int index, struct page_list **last_node)
{
        int i;
        struct page_list *target;
        struct page_list *pre;

        ZDBG("%s start. Part of page reordering\n", __FUNCTION__);

        target = job->pages;
        pre = job->pages;

        if (index == BLOCKS_PER_SEGMENT - 1) {  // target is the last node, do nothing
                return;
        } else if (index == 0) {  // need to take care of this case because there is no dummy head
                 job->pages = job->pages->next;
                 target->next = NULL;
                 (*last_node)->next = target;
                 *last_node = target;
        } else {
                for (i = 0; i < index - 1; i++) {
                        pre = pre->next;
                }
                target = pre->next;
                BUG_ON(!target);

                BUG_ON((*last_node)->next);  // zgy: I have checked the page allocation source code, this ->next should be NULL
                pre->next = target->next;
                target->next = NULL;
                (*last_node)->next = target;
                *last_node = target;
        }

        ZDBG("%s end. Part of page reordering\n", __FUNCTION__);
}

/*
 * seg_gc_set_write_info_noirq - callback of GC read
 * @dst: the destination info that we need to fill here
 * @context: io_job struct that we passed to kcopyd previously
 *
 * Description:
 *   Here we need to do some hacking on kcopyd.
 *   kcopyd is created to simply copy data from one device to another.
 *   However, we do not want to write all the data that we have read from src
 *   device. We need to change the content in pages here and also change number
 *   of sector that we want to write to the corresponded value.
 *
 *   We do not use all the page we have got. Since kcopyd does not check the
 *   number of pages and only use the information in dst. I think this approach
 *   is okay.
 */
void seg_gc_set_write_info_noirq(int *dst_count,
                            struct dm_io_region *dst,
                                 void *context)
{
        struct io_job *io = (struct io_job *) context;
        struct dm_gecko *dmg = io->dmg;
        int i;
        unsigned int real_nr_valid_blk;

        struct dm_dev_seg *src_dev = sector_to_dev(dmg, block_to_sector(io->l_block));
        struct dm_dev_seg *dest_dev = io->dest_dev;  // zgy: target we need
        unsigned int seg_idx = io->seg_idx;
        u32 start_blk = io->l_block; // l_block is the start_blk of the victim segment

        /* kcopyd_job is the struct that kcopyd uses to process request.
        * If we want to control kcopyd, we change kcopyd_job*/
        struct kcopyd_job *job = io->copy_job;

        int nr_valid_blk;
        struct valid_blks_info *valid_blks;
        u32 src_blk;
        u32 mapped_v_blk;

        struct page_list *last_page;
        unsigned int offset;

        //debug
        static int count = 0;
        ZDBG("zgy: %s, %d GC read has finished !!! Working on preparing write info\n", __FUNCTION__, ++count);

        BUG_ON(in_interrupt());  // executes by workqueue, should be user context

        nr_valid_blk = BLOCKS_PER_SEGMENT - atomic_read(&src_dev->sit[seg_idx]->nr_invalid_blk);
        if (nr_valid_blk == 0) {
                goto fast_reclaim;
        }

        valid_blks = (struct valid_blks_info *)kmalloc(sizeof(struct valid_blks_info) * nr_valid_blk, GFP_KERNEL);
        if(!valid_blks) {
                printk("%s, cannot allocate memeory for valid_blks\n", __FUNCTION__);
                BUG_ON(1);
        }

        /* go through the map to find all valid blocks.
         * for every valid blocks, also check the hash table to see if there are any pending write requests.
         * if there are, leave the block alone.
         */
        //ZDBG("%s, before get the last_page\n", __FUNCTION__);
        last_page = job->pages;
        while (last_page->next) {
                last_page = last_page->next;
        }

        offset = 0;
        real_nr_valid_blk = 0;
        for (i = 0; i < BLOCKS_PER_SEGMENT; i++) {
                src_blk = start_blk + i;
                //if (!is_block_marked_free(dmg->r_map[src_blk], dmg)) {  // valid blks get in
                mapped_v_blk = get_rmap_element(dmg, src_blk);
                if (!is_block_free_or_invalid(mapped_v_blk, dmg)) {  // valid blks get in
                        BUG_ON(dmg->d_map[mapped_v_blk] != src_blk);  // check if they are mutual mapped
                        if (need_move2(dmg, src_blk, mapped_v_blk)) {  // see if there are ongoing reqs. Add io4b to block following reqs
                                valid_blks[real_nr_valid_blk].v_blk = mapped_v_blk;
                                //valid_blks[real_nr_valid_blk].l_blk = dmg->size;  // we will set new value later
                                real_nr_valid_blk++;
                        } else {
                                move_to_the_end(job, i - offset, &last_page);
                                offset++;
                        }
                } else {
                        move_to_the_end(job, i - offset, &last_page);
                        offset++;
                }
        }

        /* Because there is no valid l_block in this segment, normal IO reqs do not access this segment.
         * Each GC job handles a different segment and we are using a single thread workqueue, there is
         * no need to use lock here. */
fast_reclaim:
        if (nr_valid_blk == 0 || real_nr_valid_blk == 0) {
                /* Reset sit entry & add it back to free_list*/
                src_dev->sit[seg_idx]->itr = 0;  // do not need to be atomic, only this job handles this seg
                atomic_set(&src_dev->sit[seg_idx]->nr_invalid_blk, 0);
                atomic_add(BLOCKS_PER_SEGMENT, &dmg->available_blocks);
                atomic_add(BLOCKS_PER_SEGMENT, &dmg->free_blocks);
                /* Do not need synchronization for IS_FIRST_SEG and free_list because
                 * this function is called by kcopyd and there is only kcopyd thread */
                if (IS_FIRST_SEG) {  /* Need to assign new free seg for this cleaned device.*/
                        src_dev->curr_seg = src_dev->sit[seg_idx];
                        IS_FIRST_SEG = not;
                } else {
                        list_add_tail(&(src_dev->sit[seg_idx]->list), &src_dev->free_list.list);
                        src_dev->free_seg_nr++;
                }
                io->nr_valid_blks = 0;

                return;
        }

        ZDBG("%s, have finished page reordering, following dest free block mapping\n", __FUNCTION__);

        // GC preemption
        // wait ongoing reads on destination disk
        wait_event(dmg->gc_pending_read_waitqueue, !atomic_read(&dest_dev->pending_reads));

        map_dst_for_seg_gc(dmg, dest_dev, dst, real_nr_valid_blk, valid_blks, job);  //zgy: allocate free block and set write info to ~dst~
        io->blks_info = valid_blks;
        io->nr_valid_blks = real_nr_valid_blk;

        ZDBG("%s, have finished dest free block mapping\n", __FUNCTION__);

        // TODO: figure out do we need to count writes on dest. I think answer is no.
        //atomic_inc(&dest_dev->pending_writes);

        //kfree(valid_blks);  // not free it, let the finish callback funtion to do it
        //debug
        ZDBG("zgy: %s, %d preparing write info finished!!\n", __FUNCTION__, count);
}

static void dm_dispatch_io_gc(struct io_job *io, u32 valid_blk)
{
        struct dm_gecko *dmg = io->dmg;
        struct dm_io_region src;
        sector_t sector;

        /* Need NOT synchonize access to the phy maps. Further, I can
           access the block map !!! to fetch the entry at index
           io->v_block since the hashtable was syncronously updated to
           indicate the gc is scheduled to run on that block and there
           are no concurrent operations on the same block while it is
           relocated by gc. This means that any further operations
           will not touch said block and therefore will not alter the
           map entry at index io->v_block. Be aware that this is NOT
           the case for dm_dispatch_io_bio, since regular reads and
           writes may be issued concurrently. */

        sector = block_to_sector(valid_blk);
        /* ziggy: get device, real sector in that device, and number of sector */
        linear_to_phy_which(dmg, sector, choose_gc_stripe(sector, dmg), &src);

        DPRINTK("Relocating [block:cnt(device-major:device-minor)] "
                "%llu:%llu(%u:%u)\n",
                (unsigned long long)sector_to_block(src.sector),
                (unsigned long long)sector_to_block(src.count),
                MAJOR(src.bdev->bd_dev), MINOR(src.bdev->bd_dev));

        //debug
        //printk("%s: a migration req of a valid block is sent out\n", __FUNCTION__);

        //debug
        // while(irqs_disabled()){
        // };

        dmg_kcopyd_copy(
            io->dmg->kcopyd_client, &src, 0, NULL, 0,
            (dmg_kcopyd_notify_fn) seg_gc_complete_callback, (void *) io,
            NULL,
            (dmg_kcopyd_notify_readdone_fn_noirq) seg_gc_complete_read_noirq);
}

#if 0
static void dm_dispatch_io_gc_whole_seg(struct io_job *io)
{
        struct dm_gecko *dmg = io->dmg;
        struct dm_io_region src;
        sector_t src_l_block, sector;

        /* Need NOT synchonize access to the phy maps. Further, I can
           access the block map !!! to fetch the entry at index
           io->v_block since the hashtable was syncronously updated to
           indicate the gc is scheduled to run on that block and there
           are no concurrent operations on the same block while it is
           relocated by gc. This means that any further operations
           will not touch said block and therefore will not alter the
           map entry at index io->v_block. Be aware that this is NOT
           the case for dm_dispatch_io_bio, since regular reads and
           writes may be issued concurrently. */
        src_l_block = dmg->d_map[io->v_block];
        BUG_ON(dmg->r_map[src_l_block] != io->v_block);
        // BUG_ON(!is_block_marked_free(io->l_block, dmg));  // l_block is not free anymore, we map it advancedly

        sector = block_to_sector(src_l_block);
        /* ziggy: get device, real sector in that device, and number of sector */
        //linear_to_phy_which(dmg, sector, choose_gc_stripe(sector, dmg), &src);
        linear_to_phy_which_read_seg(dmg, sector, choose_gc_stripe(sector, dmg), &src);

        DPRINTK("Relocating [block:cnt(device-major:device-minor)] "
                "%llu:%llu(%u:%u)\n",
                (unsigned long long)sector_to_block(src.sector),
                (unsigned long long)sector_to_block(src.count),
                MAJOR(src.bdev->bd_dev), MINOR(src.bdev->bd_dev));

        dmg_kcopyd_copy(
            io->dmg->kcopyd_client, &src, 0, NULL, 0,
            (dmg_kcopyd_notify_fn) seg_gc_whole_seg_complete_callback, (void *) io,
            NULL,
            (dmg_kcopyd_notify_readdone_fn_noirq) seg_gc_set_write_info_noirq);
}
#endif

static void memcpy_bio_into_page(struct io_job *io)
{
        struct bvec_iter iter;
        struct bio_vec bvec;
        struct bio *bio = io->bio;
        char *addr =
            io->page + to_bytes(bio->bi_iter.bi_sector & GECKO_SECTOR_TO_BLOCK_MASK);

        bio_for_each_segment(bvec, bio, iter) {
                unsigned long flags;
                /* I wonder if I can use page_address(->bv_page) +
                 * ->bv_offset instead of kmaps. */

                char *bio_addr = bvec_kmap_irq(&bvec, &flags);

                memcpy(addr, bio_addr, bvec.bv_len);

                bvec_kunmap_irq(bio_addr, &flags);
                addr += bvec.bv_len;
        }
}

static void memcpy_page_into_bio(struct io_job *io)
{
        struct bvec_iter iter;
        struct bio_vec bvec;
        struct bio *bio = io->bio;
        char *addr = io->page + to_bytes(bio->bi_iter.bi_sector & GECKO_SECTOR_TO_BLOCK_MASK);

        bio_for_each_segment(bvec, bio, iter) {
                unsigned long flags;
                // char *bio_addr = bvec_kmap_irq(bvec, &flags);
                char *bio_addr = bvec_kmap_irq(&bvec, &flags);
                // memcpy(bio_addr, addr, bvec->bv_len);
                memcpy(bio_addr, addr, bvec.bv_len);
                bvec_kunmap_irq(bio_addr, &flags);
                // addr += bvec->bv_len;
                addr += bvec.bv_len;
        }
}

static void io_complete_callback(unsigned long err, void *context)
{
        struct io_job *io = (struct io_job *)context;
        struct dm_gecko *dmg = io->dmg;
        struct dm_gecko_stats *stats;
        struct io_for_block *io4b = io->parent;
        int read_modify_write = 0;
        //int last_in_blk = 0;
        //unsigned long flags_job;
        //unsigned long io4b_flags;
        //unsigned long table_flags;
        spinlock_t *htable_lock;
        //int free_io4b = 0;

        htable_lock = get_lock_for_table(dmg, io->v_block);

        /* preemption is disabled under spinlock */
        if (err) {
                get_cpu();
                stats = this_cpu_ptr(dmg->stats);

                if (io->rw == READ) {
                        zero_fill_bio(io->bio);
                        DPRINTK("read error, returning 0s");
                        ++stats->read_err;
                } else {
                        ++stats->write_err;
                        // TODO: perhaps keep the older block instead
                        __relocate_written_block(io);
                }
                put_cpu();
        } else {
                if (io->rw == WRITE) {
                        __relocate_written_block(io);
                         /* also wake up the dmg->pending_write_waitqueue to start GC
                          * the is_last determine part is atomiced, it is okay to work without lock */
                        //__touch_new_head_seg(dmg, io->l_block);
                        __touch_new_head_seg(dmg, io->dest_dev);
#ifdef DROP_WRITE_WRITE_CLASH_OPTIMIZATION
                        clear_bit(WRITE_CLASH_IO_FOR_BLOCK, &io4b->flags);
#endif
                } else {  // READ
                        /* for GC preemptive.
                         * We will hold GC on in two points:
                         * 1. There are ongoing read on GC source disk
                         * 2. There are ongoing reads on GC destination disk*/
                        if (atomic_dec_and_test(&io->dest_dev->pending_reads)) {
                                wake_up(&dmg->gc_pending_read_waitqueue);
                        }
                }
        }

        //spin_lock_irqsave(&io4b->lock, io4b_flags);
        spin_lock_bh(&io4b->lock);
        BUG_ON(io4b->rw_cnt <= 0);
        list_del(&io->list);        /* deleted from io4b->pending_io */

        /* only check check the deferred_io list when there is no GC because
         * GC want to block following requrests until GC finished. */
        if ((--io4b->rw_cnt) == 0 && io4b->gc_while_pending == 0) {  /* no normal reqs or GC */
                BUG_ON(!list_empty(&io4b->pending_io));

                /* Only resend deferred request when there is no GC on this block */
                if (!list_empty(&io4b->deferred_io)) {  // if there is any request to the same v_block
                        struct io_job *rw_io_job, *tmp;
                        //struct deferred_stats *def_stats;
                // profiling
                #if ZGY_PROFILING
                        struct timespec64 my_clock2[2];
                        ktime_get_ts64(&my_clock2[0]);
                #endif
                //
                        //spin_lock_irqsave(&jobs_lock, flags_job);
                        spin_lock_bh(&jobs_lock);
#ifdef ENABLE_STATS
                        def_stats = this_cpu_ptr(&deferred_stats);
#endif
                        list_for_each_entry_safe(rw_io_job, tmp,
                                                &io4b->deferred_io, list) {  // ziggy: add there io to global deferred_io queue
                                list_del(&rw_io_job->list);
                                __add_deferred_io_job(rw_io_job);
#ifdef ENABLE_STATS
                                ++def_stats->total;
#endif
                        }
                        //spin_unlock_irqrestore(&jobs_lock, flags_job);
                        spin_unlock_bh(&jobs_lock);
                        wake_deferred_wqueue();
                                // profiling
                #if ZGY_PROFILING
                        ktime_get_ts64(&my_clock2[1]);
                        calclock(my_clock2, &gc_complete_joblock_time, &gc_complete_joblock_cnt);
                #endif
                //} else {  // when there is no other reqs on the v_block, we can free the io4b
                }

                //spin_lock_irqsave(htable_lock, table_flags);  // watch out, this op modifies htable, not the entry
                spin_lock_bh(htable_lock);  // watch out, this op modifies htable, not the entry
                list_del(&io4b->hashtable);
                //free_io4b = 1;
                //spin_unlock_irqrestore(htable_lock, table_flags);
                spin_unlock_bh(htable_lock);
                atomic_dec(&dmg->htable_size);
        }
        /* we cannot free this table entry lock before we delete io4b from htable.
         * because after we release the lock, RW thread may get this entry and
         * incease the rw_cnt again, however, after switch back to this thread,
         * we will delete the io4b from the htable.*/
        //spin_unlock_irqrestore(&io4b->lock, io4b_flags);
        spin_unlock_bh(&io4b->lock);
        //if (free_io4b) {
                mempool_free(io4b, io_for_block_mempool);
        //}

        if (io->page != NULL) {
                if (err) {
                        free_page((unsigned long)io->page);
                        io->page = NULL;
                } else {
                        if (io->rw == READ && bio_data_dir(io->bio) == WRITE) {
                                memcpy_bio_into_page(io);
                                io->rw = WRITE;
                                /* resubmit IO (read-modify-write) */
                                queue_deferred_io_job(io);
                                read_modify_write = 1;
                        } else {
                                if (bio_data_dir(io->bio) == READ) {
                                        memcpy_page_into_bio(io);
                                }
                                free_page((unsigned long)io->page);
                                io->page = NULL;
                        }
                }
        }

        if (!read_modify_write) {
                bio_endio(io->bio);
                mempool_free(io, io_job_mempool);
                do_complete_generic(dmg);
        }

        // If the v_block is also a GC target, GC will wait for it
        if (atomic_read(&dmg->gc_wait_for_req)) {
                wake_up(&dmg->gc_waitqueue);
        }
        // profiling
        ktime_get_ts64(&rw_clock[1]);
        calclock2(rw_clock, &total_rw_time, &total_rw_count);  // keep get the newest interval time
}

/* WARNING: do NOT touch any of the shared state (e.g. the direct and
 * reverse relocation maps) from this function---accessing the members
 * of the io_job passed in is safe, e.g. io->v_block or
 * io->l_block. The context (parameter passed to the callback) is the
 * io_job. */
static int dm_dispatch_io_bio(struct io_job *io)
{
        struct dm_gecko *dmg = io->dmg;
        struct dm_io_request iorq;
        struct dm_io_region where[DM_GECKO_MAX_STRIPES];
        struct bio *bio = io->bio;
        sector_t sector;
        int num_regions = 1;
        int flags;
        /* for GC preempty */
        struct dm_dev_seg *target_dev = sector_to_dev(dmg, block_to_sector(io->l_block));
        io->dest_dev = target_dev;  // help complete callback to fast update pending_writes/reads

                //atomic_inc(&sector_to_dev(dmg, block_to_sector(io->l_block))->pending_writes);  // if pending_write not equal 0, GC will not start on this disk
        /* The physical map requires no synchronization since it is
         * initialized once and not altered henceforth. Further, the
         * dm_io_region(s) can be allocated on-stack even though the
         * dm_io is asynchronous since it is used to set the fields of
         * a newly allocated bio (which is itself submitted for io
         * through the submit_bio() interface). WARNING! do not touch
         * the virtual and linear maps since reads and writes may be
         * issued concurrently (that's the contract at the
         * block-level---request ordering is not ensured. */

        // the sector is the same for both READ and WRITE
        sector = block_to_sector(io->l_block);
        if (io->rw == READ) {
                num_regions = 1;
                linear_to_phy_which(dmg, sector,
                                    choose_read_stripe(sector, dmg), where);

                atomic_inc(&target_dev->pending_reads);

                DPRINTK("in %s, READ <dev %u:%u> sector: %llu count: %llu",
                        __FUNCTION__,
                        MAJOR(where[0].bdev->bd_dev),
                        MINOR(where[0].bdev->bd_dev),
                        (unsigned long long)where[0].sector,
                        (unsigned long long)where[0].count);
        } else {
                linear_to_phy_all(dmg, sector, where, &num_regions);  // ziggy: write to all devices within one segment. But we only has one.
                //BUG_ON(num_regions > dmg->disk_map.stripes);
                // This is normal write from user
                atomic_inc(&target_dev->pending_writes);

                DPRINTK("in %s, WRITE <dev %u:%u> sector: %llu count: %llu num_dests: %u",
                        __FUNCTION__,
                     MAJOR(where[0].bdev->bd_dev),
                     MINOR(where[0].bdev->bd_dev),
                     (unsigned long long)where[num_regions - 1].sector,
                     (unsigned long long)where[num_regions - 1].count,
                     num_regions);
        }

#if LINUX_VERSION_CODE <= KERNEL_VERSION(2, 6, 24)
#error "Kernel version unsuported (too old)."
#elif LINUX_VERSION_CODE <= KERNEL_VERSION(2, 6, 28)
        flags = 0; //(1 << BIO_RW_SYNC);
#elif LINUX_VERSION_CODE <= KERNEL_VERSION(2, 6, 35)
        flags = 0; //(1 << BIO_RW_SYNCIO) | (1 << BIO_RW_UNPLUG);
#else
        flags = 0; //(1 | REQ_SYNC | REQ_UNPLUG);
#endif
        iorq.bi_op = io->rw;
        iorq.bi_op_flags = flags;

        if (io->page != NULL) {
                /* unaligned request */
                iorq.mem.type = DM_IO_KMEM;
                iorq.mem.ptr.addr = io->page;
                iorq.mem.offset = 0;  // only required for DM_IO_PAGE_LIST
        } else {
                iorq.mem.type = DM_IO_BIO;
                iorq.mem.ptr.bio = bio;
        }
        iorq.notify.fn = io_complete_callback;
        iorq.notify.context = io;
        iorq.client = dmg->io_client;

        /* The beauty of the log structure is that I need not maintain
         * consistent ordering of write requests across mirrors, since
         * all writes are performed against fresh blocks and no
         * in-place modifications take place. For a conventional block
         * device, two writes may be issued concurrently (e.g.  by
         * uncooperating processes while bypassing the buffer cache w/
         * O_DIRECT) and while this may be perfectly fine for a block
         * device backed up by a single disk, it may be an issue for a
         * RAID-1 array. For example, dm_io -> async_io -> dispatch_io
         * -> do_region will call submit_bio for every mirror disk,
         * which means that concurrent requests A and B for the same
         * block X mirrored on devices D1 and D2 may be queued to the
         * respective elevators in any order (e.g. A, B for D1 and B,
         * A for D2). This means that a write-write conflict will
         * break the RAID-1.  The log structure needs not solve this
         * issue at all, since by construction, the concurrent
         * requests A and B for the same (virtual) block X will be
         * mapped down to different linear blocks, and the latter
         * request will persist correctly.
         *
         * Note that #ifdef DROP_WRITE_WRITE_CLASH_OPTIMIZATION then
         * the above write-write conflict will not even occur since if
         * concurrent writes are issued to the same block, only the
         * first will succeed. We can get away with this optimization
         * since POSIX does not guarantee any ordering of writes for
         * uncooperating processes issuing concurrent writes.
         */

        DPRINTK("%s, dm_io finished!\n");
        return dm_io(&iorq, num_regions, where, NULL);
}

static int zone_compare_greedy(const void *lhs, const void *rhs)
{
        struct smr_zone *lhs_zone = (const struct smr_zone *)(lhs);
        struct smr_zone *rhs_zone = (const struct smr_zone *)(rhs);

        int lhs_value = atomic_read(&lhs_zone->nr_invalid_blk);
        int rhs_value = atomic_read(&rhs_zone->nr_invalid_blk);

        if (lhs_value < rhs_value) return 1;
        if (lhs_value > rhs_value) return -1;
        return 0;
}

/*
 * (zgy): We use pre-defined strategy to list segments that need to be cleaned.
 *
 * For example, here, we use the greedy strategy that select the segment with
 * leaste number of valid blocks as the victim segment.
 * Because we need to do several segment cleaning, the list segment idxs in
 * @targets in the order.
 */
void dev_gc_targets(struct dm_dev_seg *dev, int *targets)
{
        //struct dm_gecko *dmg = dev->ctxt;
        int nr_zones = dev->total_seg_nr >> SEG_TO_ZONE_SHIFT;
        struct smr_zone *tmp_zones;
        int i;

        tmp_zones = (struct smr_zone *)kmalloc(
                sizeof(struct smr_zone) * nr_zones, GFP_KERNEL);

        /* ziggy: sorting and get the number of target segments we want.
         * The overhead is okay since we only do this once for this device*/
        for (i = 0; i < nr_zones; i++) {  // copy all the sit entries.
                memcpy(&tmp_zones[i], &dev->zones[i], sizeof(struct smr_zone));
        }

        sort(tmp_zones, nr_zones, sizeof(struct smr_zone), &zone_compare_greedy, NULL);

        for (i = 0; i < nr_zones; i++) { // we only get the index because we copyed new segments to avoid locking
                targets[i] = tmp_zones[i].zone_idx;
        }

        kfree(tmp_zones);  /* We only need the segment indices */

        return;
}

//u32 block_from_seg(struct dm_dev_seg *dev);
u32 block_from_seg(struct dm_dev_seg *dev, int *need_gc);
/* (zgy): Map destination block for valid blocks in victim segment.
 *
 * Right now we only move valid pages to body disks, so this is simply allcoate
 * next write head of a certain disk.
 */
static inline u32 dest_blk_for_gc(struct dm_gecko *dmg, struct dm_dev_seg *dest_dev)
{
    int dummy = 0;
       //unsigned short dev_idx = dmg->writing_dev;
       BUG_ON(dest_dev->idx == dmg->writing_dev);
       u32 next_free_blk = block_from_seg(dest_dev, &dummy);

       return next_free_blk;
}

/*
 * (zgy): Create a gc io_job and io4b for a valid block in the victim segment.
 *
 * First, we check if there is pending request on this block.
 * If there is pending writing, we leave this block alone.
 * If there is pending reading, we start gc. However, we mark GC on this v_block
 * block following request on this block to deferred_io list.
 *
 * Second, we map destination for valid block then send them out.
 */
static void create_blk_gc_job(struct valid_blks_info valid_blk,
                              struct dm_gecko *dmg,
                              struct dm_dev_seg *dest_dev,
                              int nr_valid_blks,
                              int seg_idx,
                              int reclaim_zone)
{
        struct io_job *gc_io;

        u32 v_blk = valid_blk.v_blk;
        u32 l_blk = valid_blk.l_blk;

#ifdef ENABLE_STATS
        struct dm_gecko_stats *stats;
        get_cpu();
        stats = this_cpu_ptr(dmg->stats);

        ++stats->gc;
        put_cpu();
#endif

        //atomic_inc(&dmg->gc_wait_for_req);

        gc_io = mempool_alloc(io_job_mempool, GFP_NOIO);
        set_io_job_gc(gc_io); /* assign bio to NULL */
        gc_io->dmg = dmg;
        gc_io->page = NULL;  /* means this is a deferred job */
        gc_io->rw = READ;
        gc_io->dest_dev = dest_dev;
        gc_io->seg_idx = seg_idx;
        gc_io->nr_valid_blks = nr_valid_blks;

        gc_io->reclaim_zone_idx = reclaim_zone;

        /* zgy: This is kind tricky. When doing GC, the v_block of io job is
        * used to indicate the valid block in victim segment; and the
        * l_block of io_job is used to indicate the destination block.
        * This is the original gecko design, however, it still fits our
        * segment GC well.*/
        gc_io->v_block = v_blk;

        /* There is no lock needed. There is only one thread working on send GC reqs.
         * And every time only one GC job allocates free block from a device. */
        gc_io->l_block = dest_blk_for_gc(dmg, dest_dev);

        atomic_inc(&dmg->nr_kcopyd_jobs);
        atomic_inc(&dmg->total_jobs);

        dm_dispatch_io_gc(gc_io, l_blk);

        return; /* Write pending. Do not need to do cleaning on it.*/
}

#if 0
static void create_seg_gc_job(sector_t l_blk,
                              struct dm_gecko *dmg,
                              struct dm_dev_seg *dest_dev,
                              int seg_idx)
{
        BUG_ON(in_interrupt());

        u32 relocated_block;
        struct io_for_block *io4b, *extant_io4b;
        struct io_job *gc_io;
        struct dm_gecko_stats *stats;

        struct dm_io_region src;
        sector_t src_l_block, sector;

        stats = this_cpu_ptr(dmg->stats);

        sector_t v_blk = dmg->r_map[l_blk];
        BUG_ON(is_block_free_or_invalid(v_blk, dmg));

        ++stats->gc;
        ++dmg->gc_req_in_progress;

        gc_io = mempool_alloc(io_job_mempool, GFP_NOIO);
        gc_io->dmg = dmg;
        set_io_job_gc(gc_io); /* assign bio to NULL */
        gc_io->page = NULL;  /* means this is a deferred job */

        gc_io->v_block = v_blk;

        dm_dispatch_io_gc(gc_io);
}
#endif

/*
 * create_gc_job_whole_seg - pass a victim segment for GC
 * @l_blk: start linear block address of the victim segment
 * @dest_dev: the device that we aim to move valid blocks to
 * @seg_idx: segment index of the victim segment. For metadata update after GC.
 *
 * Description:
 *   Create job struct for kcopyd client. The whole segment is read from disk
 *   first, then write valid blocks to destination device.
 *   We do not map read destination addresses here because we want to reduce
 *   the locking time and avoid unnecessary movement as many as possible.
 *   (more invalid block may occurs during the reading.)
 */
static void create_gc_job_whole_seg(u32 seg_start_l_blk,
                                    struct dm_gecko *dmg,
                                    struct dm_dev_seg *dest_dev,
                                    int seg_idx,
                                    int reclaim_zone_idx)
{
        struct io_job *gc_io;
        struct dm_gecko_stats *stats;
        //unsigned long flags;

        struct dm_io_region src;
        sector_t sector;

        //atomic_inc(&dmg->gc_wait_for_req);

#ifdef ENABLE_STATS
        get_cpu();
        stats = this_cpu_ptr(dmg->stats);
        ++stats->gc;
        put_cpu();
#endif

        gc_io = mempool_alloc(io_job_mempool, GFP_NOIO);
        gc_io->dmg = dmg;
        set_io_job_gc(gc_io); /* assign bio to NULL */
        gc_io->page = NULL;  /* means this is a deferred job */
        gc_io->parent = NULL;  /* we do not connect whole segment GC req with io4b */

        gc_io->v_block = NULL;  /* Has no meaning because we do not use this field */
        gc_io->l_block = seg_start_l_blk;  // readdone callback use this to find src_dev
        gc_io->dest_dev = dest_dev;
        gc_io->seg_idx = seg_idx;

        gc_io->reclaim_zone_idx = reclaim_zone_idx;

        sector = block_to_sector(seg_start_l_blk);
        // zgy: assign IO info to ~src~
        //linear_to_phy_which_read_seg(dmg, sector, choose_gc_stripe(sector, dmg), &src);  // information in ~src~
        linear_to_phy_which_read_seg(dmg, sector, 0, &src);  // information in ~src~

        atomic_inc(&dmg->nr_kcopyd_jobs);
        atomic_inc(&dmg->total_jobs);  // # of io_job objects

        dmg_kcopyd_copy(
            gc_io->dmg->kcopyd_client, &src, 0, NULL, 0,
            (dmg_kcopyd_notify_fn) seg_gc_whole_seg_complete_callback,
            (void *) gc_io,
            NULL,
            (dmg_kcopyd_notify_readdone_fn_noirq) seg_gc_set_write_info_noirq
        );
        //debug
        //printk("%s: gc req for a whole segment has been sent", __FUNCTION__);

        return;
}

/*
 * zgy:
 * @seg_idx is the segment index of the given device.
 * This device is the previous writing device. After swap out, we do cleaning on
 * this device to reclaim free space.
 *
 * Currently, the stategy is very simple. We just move every valid blk in this
 * segment to one of the body device.
 */
static void dispatch_seg_single(struct dm_dev_seg *src_dev,
                                unsigned int seg_idx,
                                int *tg_dev_idx,
                                int reclaim_zone)
{
        int i, j;
        u32 src_blk;
        int nr_valid_blk;
        struct valid_blks_info *valid_blks;
        struct dm_gecko *dmg = src_dev->ctxt;
        u32 base_blk = sector_to_block(src_dev->start);
        u32 seg_start_blk = seg_idx * BLOCKS_PER_SEGMENT;
        u32 start_blk = seg_start_blk + base_blk;

        u32 mapped_v_blk;
        unsigned int real_nr_valid_blk;

        struct dm_dev_seg *dest_dev = NULL;

        // profiling
#if ZGY_PROFILING
        struct timespec64 my_clock[2];
        ktime_get_ts64(&my_clock[0]);
#endif

        nr_valid_blk = BLOCKS_PER_SEGMENT - atomic_read(&src_dev->sit[seg_idx]->nr_invalid_blk);
        valid_blks = (struct valid_blks_info*)kmalloc(sizeof(struct valid_blks_info) * nr_valid_blk, GFP_KERNEL);
        if (!valid_blks) {
            BUG_ON(1);
        }
         /* Check the hashtable to see if there is any pending overwrite on valid blocks.
          * If there are, we leave these valid blocks alone because they will be invalid
          * in the very near future.*/
        real_nr_valid_blk = 0;
        for (i = 0; i < BLOCKS_PER_SEGMENT; i++) {
                src_blk = start_blk + i;
                mapped_v_blk = get_rmap_element(dmg, src_blk);
                if(!is_block_marked_free(mapped_v_blk, dmg)) {
                        /* zgy: make sure there is no pending writes on the v_blk before we send the req */
                        if (need_move2(dmg, src_blk, mapped_v_blk)) {  // only check hashtable, does not insert io4b
                            valid_blks[real_nr_valid_blk].v_blk = mapped_v_blk;
                            valid_blks[real_nr_valid_blk].l_blk = src_blk;
                            real_nr_valid_blk++;
                        }
                }
        }

        /* Because there is no valid l_block in this segment, normal IO reqs do not access this segment.
         * Each GC job handles a different segment and we are using a single thread workqueue, there is
         * no need to use lock here. */
         //TODO: handle zone reset
        if (real_nr_valid_blk == 0) {
                /* Reset sit entry & add it back to free_list*/
                src_dev->sit[seg_idx]->itr = 0;
                atomic_set(&src_dev->sit[seg_idx]->nr_invalid_blk, 0);
                atomic_add(BLOCKS_PER_SEGMENT, &dmg->available_blocks);
                atomic_add(BLOCKS_PER_SEGMENT, &dmg->free_blocks);
                if (IS_FIRST_SEG) {  /* Need to assign new free seg for this cleaned device.*/
                        src_dev->curr_seg = src_dev->sit[seg_idx];
                        IS_FIRST_SEG = not;
                } else {
                        list_add_tail(&(src_dev->sit[seg_idx]->list), &src_dev->free_list.list);
                        src_dev->free_seg_nr++;
                }
                return;
        }

        // GC preemption
        // check reading in the victim disk, let normal READ go first
        wait_event(dmg->gc_pending_read_waitqueue, !atomic_read(&src_dev->pending_reads));

#if ZGY_PROFILING
        ktime_get_ts64(&my_clock[1]);
        calclock(my_clock, &dispatch_dmglock_time, &dispatch_seg_cnt);
#endif
        /* Map target device.
         * We may send the valid to head disk, however, MUST do not send
         * it back to this GCing disk again */
next_disk:
        while (*tg_dev_idx == dmg->writing_dev || *tg_dev_idx == src_dev->idx) {
                *tg_dev_idx = (*tg_dev_idx + 1) % dmg->disk_map.cnt;
        }
        dest_dev = dev_for_idx(dmg, *tg_dev_idx);
        if (!dest_dev->free_seg_nr) {  // avoid disk that has no free space
                *tg_dev_idx = (*tg_dev_idx + 1) % dmg->disk_map.cnt;
                goto next_disk;
        }

        if (dest_dev == NULL) {
                printk("!!! In %s, Bad device index: %d. This is an invalid idx\n",__FUNCTION__, *tg_dev_idx);
                //printk("!!! In %s, Bad device index: %d. This is an invalid idx\n",__FUNCTION__, tg_dev_idx);
                BUG_ON(dest_dev == NULL);
        }
        BUG_ON(dest_dev == dmg->head_seg);

        /* What if there is no enough space for these valid block? */
        for (j = 0; j < real_nr_valid_blk - 1; j++) {
                create_blk_gc_job(valid_blks[j], dmg, dest_dev, 0, -1, reclaim_zone);
        }
        create_blk_gc_job(valid_blks[j], dmg, dest_dev, real_nr_valid_blk, seg_idx, reclaim_zone);  /* The last valid block in the victim segment */

        *tg_dev_idx = (*tg_dev_idx + 1) % dmg->disk_map.cnt;

        kfree(valid_blks);

        //debug
        //printk("end of %s\n", __FUNCTION__);
}

static inline void dispatch_seg_whole(struct dm_dev_seg *src_dev,
                                      unsigned short seg_idx,
                                      int *tg_dev_idx,
                                      int reclaim_zone)
{
        struct dm_gecko *dmg = src_dev->ctxt;

        // get the start linear sector of the victim segment
        u32 base_blk = sector_to_block(src_dev->start);
        u32 seg_start_blk = seg_idx * BLOCKS_PER_SEGMENT;
        u32 start_blk = seg_start_blk + base_blk;
        struct dm_dev_seg *dest_dev = NULL;

        /* Map target device.
         * We may send the valid to head disk, however, MUST do not send
         * it back to this GCing disk again */
next_disk:
        while (*tg_dev_idx == dmg->writing_dev || *tg_dev_idx == src_dev->idx) {
                *tg_dev_idx = (*tg_dev_idx + 1) % dmg->disk_map.cnt;
        }
        dest_dev = dev_for_idx(dmg, *tg_dev_idx);
        if (!dest_dev->free_seg_nr) {  // avoid disk that has no free space
                *tg_dev_idx = (*tg_dev_idx + 1) % dmg->disk_map.cnt;
                goto next_disk;
        }
        if (dest_dev == NULL) {
                printk("!!! In %s, Bad device index: %d. This is an invalid idx\n",__FUNCTION__, *tg_dev_idx);
                BUG_ON(1);
        }
        BUG_ON(src_dev->idx == dest_dev->idx);

        // GC preemption
        // TODO: the best place to wait for this even is after need_move2()
        // However, the whole_seg GC reads the segment to memory first,
        // then determine if we need to write.
        // I can only do this here... maybe
        wait_event(dmg->gc_pending_read_waitqueue, !atomic_read(&src_dev->pending_reads));

        create_gc_job_whole_seg(start_blk, dmg, dest_dev, seg_idx, reclaim_zone);
       
        *tg_dev_idx = (*tg_dev_idx + 1) % dmg->disk_map.cnt;
}

/*
 * (zgy): This is the cleaning work job.
 * When allocating blocks for write bio, once the current writing disk is run out
 * of free blocks and we switch to next disk, we add this job into cleaning workqueue.
 */
/* This work is binding to a single workqueue.
 * Thus, there is no synchronization issue by itself.*/
void start_gc_on_dev(struct work_struct *work)
{
        // for time profiling
#if ZGY_PROFILING
        total_gc_time += gc_running_time;
        ktime_get_ts64(&gc_clock[0]);
#endif

        int i;
        int tg_dev;
        int nr_invalid_blk;
        struct dm_dev_seg *dev = container_of(work, struct dm_dev_seg, cleaning_work);
        struct dm_gecko *dmg = dev->ctxt;

        /* waiting pending writes on this device to finish*/
        /* 1. check pending_write on this dev
         * 2. sleep */
        wait_event(dmg->pending_write_waitqueue, !atomic_read(&dev->pending_writes));

        /*
        DECLARE_WAITQUEUE(__wait_gc, current);  // current is a macro that gives the task_structure of current process
                                                // this macro return a entry instead of a waitqueue

        if (!atomic_read(&dev->pending_writes)) {
                goto start;
        }
        // Wait the last writing on the victim disk to finish.
        __add_wait_queue(&dmg->pending_write_waitqueue, &__wait_gc);
        for (;;) {
                printk("zgy: in %s, get stuck in the waiting queue!!!!!\n", __FUNCTION__);

                __set_current_state(TASK_UNINTERRUPTIBLE);

                if (!atomic_read(&dev->pending_writes)) {
                        break;
                }
                BUG_ON(in_interrupt());
                schedule();
        }
        __set_current_state(TASK_RUNNING);
        __remove_wait_queue(&dmg->pending_write_waitqueue, &__wait_gc);
        */
start:
        //debug
        printk("All pending writes on the victim device is finished\n");

        //  1. find victim segments
        tg_dev = 0;

        int nr_zones = dev->total_seg_nr >> SEG_TO_ZONE_SHIFT;
        int *sorted_zone_list = (int *)kmalloc(sizeof(int) * nr_zones, GFP_KERNEL);
        /* greedy strategy. The zone with less number of valid blocks are cleaned first */
        dev_gc_targets(dev, sorted_zone_list);

        int gc_zone_limit = nr_zones * 2 / 10 ;  // 20%
        for (i = 0; i < gc_zone_limit; i++) {
                int zone_idx;
                int nr_segs = (1 << SEG_TO_ZONE_SHIFT);
                int start_seg;
                int end_seg;
                int j;

                zone_idx = sorted_zone_list[i];
                start_seg = zone_idx << SEG_TO_ZONE_SHIFT;
                end_seg = start_seg + nr_segs;
                for (j = start_seg; j < end_seg; j++) {
                        /* control the number of concurrent kcopyd jobs. May not necessary  */
                        wait_event(dmg->kcopyd_job_waitqueue, atomic_read(&dmg->nr_kcopyd_jobs) < 20);

                        nr_invalid_blk = atomic_read(&dev->sit[j]->nr_invalid_blk);  // GC from the first to the last segment
                        /* When reading a 1MB chunk, our disk can reach the full bandwidth -- 160MB/s.
                        * When send 4k randread reqeust, the throughput of our disk is about 5MB/s.
                        * Let's assum it can reach to 10MB/s.
                        *
                        * If the # of valid block greater than 64, read a whole segment is faster. */
                        //if (nr_invalid_blk >= BLOCKS_PER_SEGMENT - 64) {
                        //        if (j == end_seg - 1) {
                        //                dispatch_seg_single(dev, j, &tg_dev, zone_idx);  // read valid blks only
                        //        } else {
                        //                dispatch_seg_single(dev, j, &tg_dev, -1);  // read valid blks only
                        //        }
                        //} else {
                                if (j == end_seg - 1) {
                                        dispatch_seg_whole(dev, j, &tg_dev, zone_idx);
                                } else {
                                        dispatch_seg_whole(dev, j, &tg_dev, -1);
                                }
                        //}
                }
        }
        kfree(sorted_zone_list);

        ZDBG("ziggy: we've clean %d zones in device %d\n", i, dev->idx);
}

unsigned long long tmp_count = 0;
u32 block_from_seg(struct dm_dev_seg *dev, int *need_gc)
{
        struct dm_gecko *dmg = dev->ctxt;
        u32 blk;
        struct dev_sit_entry *free_seg;
        unsigned short old_dev_idx;
        u32 base_blk;
        struct dm_dev_seg *dst_dev;

        blk = dev->curr_seg->seg_nr * BLOCKS_PER_SEGMENT + dev->curr_seg->itr;

        // GC writes and normal writes are heading to different disks.
        atomic_dec(&dmg->available_blocks);
        atomic_dec(&dmg->free_blocks);

        if (is_last_block_in_seg(dev->curr_seg->itr)) {  // allocate new segment or move to other dev
                if (dev->free_seg_nr) {
                        free_seg = list_first_entry(&(dev->free_list.list), struct dev_sit_entry, list);
                        list_del(&free_seg->list);
                        dev->curr_seg = free_seg;
                        dev->free_seg_nr--;  // the caller has already hold the lock
                } else {  // This device is full, switch to next one.
                        // (ziggy): GC procedure also use this function to
                        // allocate blocks (?), we need to distinguish the request
                        // is from normal IO or GC.
                        if (dev->idx == dmg->writing_dev) { // Normal IO
                                int next_dev;

                                old_dev_idx = dmg->writing_dev;
                                next_dev = (dmg->writing_dev + 1) % dmg->disk_map.cnt;

                                // The next device may be full
                                while(1) {
                                        dst_dev = dev_for_idx(dmg, next_dev);
                                        if (dst_dev->free_seg_nr) {  // avoid disk that has no free space
                                                break;
                                        }
                                        next_dev = (next_dev + 1) % dmg->disk_map.cnt;
                                }
                                dmg->writing_dev = next_dev;

                                // Debug
                                printk("ziggy: switched from %hu to device %hu !!!\n", old_dev_idx, dmg->writing_dev);
                                /* GC starts from here */
                                IS_FIRST_SEG = is;  /* help assign new curr_seg for this cleaned disk */
                                *need_gc = 1;
                                //queue_work(gecko_cleaning_wqueue, &dev->cleaning_work);
                        } else { // During GC
                                // TODO(ziggy): leave this case right now
                                printk("!!! In function %s, unfinished implementation !!!\n", __FUNCTION__);
                                BUG_ON(1);
                        }
                }
        } else {  // allocate next block in current segment
                dev->curr_seg->itr++;
        }

        base_blk = sector_to_block(dev->start);

        return blk + base_blk;
}

/*
 * Get next free block from current writing (head) device
 * Each device maintains its own free segments list.
 */
 /* ->lock is held by upper level (mapping for normal io) */
static u32 allocate_next_free_block(struct dm_gecko *dmg)
{
       u32 block_nr;
       //unsigned long flags;
       unsigned short dev_idx;
       int need_gc = 0;

       //spin_lock_irqsave(&dmg->write_dev_lock, flags);
       //spin_lock_bh(&dmg->write_dev_lock);
       spin_lock(&dmg->write_dev_lock);

       dev_idx = dmg->writing_dev;
       struct dm_dev_seg *dev = dev_for_idx(dmg, dev_idx);
       BUG_ON(!dev);

       block_nr = block_from_seg(dev, &need_gc);
       //spin_unlock_irqrestore(&dmg->write_dev_lock, flags);
       //spin_unlock_bh(&dmg->write_dev_lock);
       spin_unlock(&dmg->write_dev_lock);

       if (need_gc) {
            queue_work(gecko_cleaning_wqueue, &dev->cleaning_work);
       }

       return block_nr;
}

static void map_rw_io_job(struct io_job *io)
{
        struct dm_gecko *dmg = io->dmg;
        //struct dm_gecko_stats *stats;
        //unsigned long flags;
        //unsigned long table_flags;
        //unsigned long io4b_flags;
        struct io_for_block *io4b, *extant_io4b;
        int dispatch_io_now = 1;
        spinlock_t *htable_lock;

        BUG_ON(in_interrupt());
        io4b = mempool_alloc(io_for_block_mempool, GFP_NOIO);
        io4b->rw_cnt = 1;
        io4b->gc_while_pending = 0;  /* for segment GC */
        io4b->flags = 0;
        INIT_LIST_HEAD(&io4b->pending_io);
        INIT_LIST_HEAD(&io4b->deferred_io);
        io->parent = io4b;
        list_add_tail(&io->list, &io4b->pending_io);

        // zgy profiling
#if ZGY_PROFILING
        struct timespec64 my_clock[2];
        ktime_get_ts64(&my_clock[0]);
#endif

        // assign l_block for this io req (valid l_blk if req is READ)
        BUG_ON(is_block_invalid(dmg->d_map[io->v_block], dmg));
        if (!is_block_marked_free(dmg->d_map[io->v_block], dmg)) {
                io->l_block = dmg->d_map[io->v_block];  /*  ziggy: map l_blk for READ. Write will get the head l_blk in following part */
                BUG_ON(get_rmap_element(dmg, io->l_block) != io->v_block);
        } else {
                /* this v_blk maps to nothing */
                io->l_block = mark_block_free(dmg);
        }

        if (io->rw == READ) {
                //++stats->reads;
                /* optimization: WARNING, it complicates the
                 * read-modify-write code paths */
                if (is_block_marked_free(dmg->d_map[io->v_block], dmg)) {
                        //++stats->read_empty;
                        //spin_unlock_irqrestore(&dmg->lock, flags);
                        if (io->page != NULL) {
                                if (bio_data_dir(io->bio) == WRITE) {
                                        clear_page(io->page);
                                        memcpy_bio_into_page(io);
                                        io->rw = WRITE;
                                        /* resubmit IO (read-modify-write) */
                                        queue_deferred_io_job(io);
                                        return;
                                } else {
                                        free_page((unsigned long)io->page);
                                        /* and continue to fall through */
                                }
                        }
                        //DPRINTK("READ unwritten blocks, returning zeroes.");
                        zero_fill_bio(io->bio);
#ifdef DROP_WRITE_WRITE_CLASH_OPTIMIZATION
out_without_submitting_io:
#endif
                        bio_endio(io->bio);
                        mempool_free(io, io_job_mempool);
                        mempool_free(io4b, io_for_block_mempool);
                        do_complete_generic(dmg);
                        return;
                }
        }

        htable_lock = get_lock_for_table(dmg, io->v_block);
/* finished table access, following is hashtable access */
refetch_entry:
        //spin_lock_irqsave(htable_lock, table_flags);  // new lock for hashtable access
        spin_lock_bh(htable_lock);  // new lock for hashtable access
        extant_io4b = get_io_for_block(dmg, io->v_block);  // ziggy: get all the IOs on the v_block
        if (!extant_io4b) {
                spin_lock_init(&io4b->lock);
                put_io_for_block(dmg, io->v_block, io4b);
                //spin_unlock_irqrestore(htable_lock, table_flags);  // release the hashtable lock as soon as possible
                spin_unlock_bh(htable_lock);  // release the hashtable lock as soon as possible

                io4b = NULL;        /* to prevent deallocation before ret */
        } else {
                //if (!spin_trylock_irqsave(&extant_io4b->lock, io4b_flags)) {
                if (!spin_trylock(&extant_io4b->lock)) {
                        //spin_unlock_irqrestore(htable_lock, table_flags);
                        spin_unlock_bh(htable_lock);
                        goto refetch_entry;
                }
                /* Need to release the htable_lock after we lock the io4b so other threads
                 * cannot get this io4b at the same time. */
                //spin_unlock_irqrestore(htable_lock, table_flags);
                spin_unlock_bh(htable_lock);

                /* unchain from io4b->pending_io, unnecessary op */
                list_del(&io->list);  // the io4b is allcoated from this function, no lock is needed
                io->parent = extant_io4b;

                /* ziggy: We added gc_while_pending so GC does not need to waiting for on going read requests.
                 * here, we check it to see if the v_blk is under GC*/
                BUG_ON(!((extant_io4b->rw_cnt != 0) || (extant_io4b->gc_while_pending == 1)));
                //if ((extant_io4b->rw_cnt != 0) || (extant_io4b->gc_while_pending == 1)) {
                        /* there is concurrent IO on this block */
#ifdef DROP_WRITE_WRITE_CLASH_OPTIMIZATION
        //TODO: I think here has some problem. The pending req can be READ, how can he know this is a
        // write write clash by only checking the io direction of the current req?
                        if (io->rw == WRITE) {
                                if (test_and_set_bit(WRITE_CLASH_IO_FOR_BLOCK,
                                                     &io4b->flags)) {
                                        //++stats->ww_clash;

                                        //spin_unlock_irqrestore(&extant_io4b->lock, io4b_flags);
                                        spin_unlock_bh(&extant_io4b->lock);
                                        if (io->page != NULL) {
                                                free_page((unsigned long)io->page);
                                                io->page = NULL;
                                        }
                                        goto out_without_submitting_io;
                                }
                        }
#endif

#ifdef ENABLE_STATS
                        stats = this_cpu_ptr(dmg->stats);
                        ++stats->rw_clash;
#endif


                         /* Here we handle all reqs that toward to the same v_block sequentially.
                         * Also, this design makes d_map lock-free because to each d_map entry, there can be
                         * only one thread accesses it at a time. */
                        dispatch_io_now = 0;
                        list_add_tail(&io->list, &extant_io4b->deferred_io);
                //}
#if 0
                else {  /* When there are several reqs on the same v_block, after the pending one finished,
                           * the io_complete funtion will resend following deferred reqs.
                           * They will come here.
                           * Actually we can ask complete callback func to delete the io4b.
                           * This is a small optimization to avoid unnecessary io4b object allocation.*/
                        list_add_tail(&io->list, &extant_io4b->pending_io);
                        extant_io4b->rw_cnt++;
                }
#endif
                //spin_unlock_irqrestore(&extant_io4b->lock, io4b_flags);
                spin_unlock_bh(&extant_io4b->lock);
        }

        if (dispatch_io_now && io->rw == WRITE) {
               /* Unlike DEFINE_WAIT, DECLARE_WAITQUEUE uses
                * default_wake_function instead of
                * autoremove_wake_function to wake up the task. The
                * former will NOT remove the woken task from the
                * wait_queue_head_t whereas the latter will. We don't
                * want the task removed. */

                wait_event(dmg->no_free_space_waitqueue, !__no_available_blocks(dmg));

                /* seems the following code block is same as wait_event
                DECLARE_WAITQUEUE(__wait, current);  // current is a macro that gives the task_structure of current process

                if (!__no_available_blocks(dmg)) {
                        goto fastpath_claim_block_for_writing;
                }

                //spin_lock_irqsave(&dmg->lock, flags);  // this lock also protect the add_wait_queue function, I think we cannot remove this
                //spin_lock_bh(&dmg->lock);  // this lock also protect the add_wait_queue function, I think we cannot remove this
                spin_lock(&dmg->lock);  // this lock also protect the add_wait_queue function, I think we cannot remove this
                __add_wait_queue(&dmg->no_free_space_waitqueue, &__wait);
                for (;;) {
                        // DEBUG
                        printk("ziggy: in %s, there is no available free block, put into waitqueue\n", __FUNCTION__);

                        __set_current_state(TASK_UNINTERRUPTIBLE);

                        if (!__no_available_blocks(dmg)) {
                                break;
                        }
                        BUG_ON(in_interrupt());
                        //spin_unlock_irqrestore(&dmg->lock, flags);
                        //spin_unlock_bh(&dmg->lock);
                        spin_unlock(&dmg->lock);

                        schedule();

                        // at this point, current is in TASK_RUNNING
                        //spin_lock_irqsave(&dmg->lock, flags);
                        //spin_lock_bh(&dmg->lock);
                        spin_lock(&dmg->lock);
                }
                __set_current_state(TASK_RUNNING);
                __remove_wait_queue(&dmg->no_free_space_waitqueue, &__wait);
                //spin_unlock_irqrestore(&dmg->lock, flags);
                //spin_unlock_bh(&dmg->lock);
                spin_unlock(&dmg->lock);
fastpath_claim_block_for_writing:
*/
                /*  write_dev_lock */
                io->l_block = allocate_next_free_block(dmg);  // move head to next block
                /* Since the io->l_block is unique, we do not need lock here */
                update_rmap(dmg, io->l_block, io->v_block);

                //atomic_inc(&sector_to_dev(dmg, block_to_sector(io->l_block))->pending_writes);  // if pending_write not equal 0, GC will not start on this disk
        }

#if ZGY_PROFILING
        ktime_get_ts64(&my_clock[1]);
        calclock(my_clock, &rw_dmglock_time, &rw_dmglock_count);
#endif

        if (io4b != NULL) {
                mempool_free(io4b, io_for_block_mempool);
        }
        if (dispatch_io_now) {
                dm_dispatch_io_bio(io);
        }
}

static int map_rw(struct dm_gecko *dmg, struct bio *bio)
{

        struct io_job *io = mempool_alloc(io_job_mempool, GFP_NOIO);

        io->bio = bio;
        io->dmg = dmg;
        io->page = NULL;

        if (!bio_at_block_boundary(bio)) {   // ziggy: when bio size is smaller than a block/page
#ifdef ENABLE_STATS
                struct dm_gecko_stats *stats;
#endif
                /* if not aligned at page boundary, must be less than
                 * a page worth of data */
                BUG_ON(bio->bi_iter.bi_size >= GECKO_BLOCK_SIZE);

                io->page = (void *)__get_free_page(GFP_NOIO);
                if (io->page == NULL) {
                        mempool_free(io, io_job_mempool);
                        bio_endio(bio);
                        goto out_ret;
                }
#ifdef ENABLE_STATS
                get_cpu();
                stats = this_cpu_ptr(dmg->stats);
                if (bio_data_dir(bio) == READ) {
                        ++stats->subblock_reads;
                } else {
                        ++stats->subblock_writes;
                }
                put_cpu();
#endif

                io->rw = READ;  /* read-modify-update cycle, read first */

                DPRINTK("in %s, %s request unaligned, sector(%llu) : size(%llu)",
                        __FUNCTION__,
                        (bio_data_dir(bio) == READ) ? "READ" : "WRITE",
                        (unsigned long long)bio->bi_iter.bi_sector,
                        (unsigned long long)bio->bi_iter.bi_size);
        } else {
                /* if aligned at page boundary, must be single block
                 * worth of data */
                BUG_ON(bio->bi_iter.bi_size != GECKO_BLOCK_SIZE);
                io->rw = bio_data_dir(bio);
        }

        // ziggy: we do not need to change this for segment because @v_block is not related to segments
        io->v_block = sector_to_block(bio->bi_iter.bi_sector);
        // the block must fit in the range
        BUG_ON(is_block_free_or_invalid(io->v_block, dmg));
        atomic_inc(&dmg->total_jobs);

        map_rw_io_job(io);

out_ret:
        return DM_MAPIO_SUBMITTED;
}

/* TRIMs are advisory, do not issue them when there are other pending
 * read/write or gc relocation/cleaning on the target block. Further,
 * they are not deferred */
static int map_discard(struct dm_gecko *dmg, struct bio *bio)
{
    //ZDBG("ziggy: start of %s %d!!!!! I have no changed this part yet.\n", __func__, __LINE__);
    //BUG_ON(1);

        //unsigned long table_flags;
        struct dm_gecko_stats *stats;
        struct io_for_block *io4b;
        u32 mapped_v_blk;
        u32 l_block;
        spinlock_t *htable_lock;
        sector_t v_block = sector_to_block(bio->bi_iter.bi_sector);

        /* never discard block 0 which holds the superblock */
        BUG_ON(v_block == 0);
        htable_lock = get_lock_for_table(dmg, v_block);
        //spin_lock_irqsave(htable_lock, table_flags);
        spin_lock_bh(htable_lock);
        /* preemption is disabled under spinlock */
        stats = this_cpu_ptr(dmg->stats);

        io4b = get_io_for_block(dmg, v_block);

        if (io4b != NULL) {
                ++stats->dropped_discards;
        } else {
                l_block = dmg->d_map[v_block];

                BUG_ON(is_block_invalid(l_block, dmg));
                if (is_block_marked_free(l_block, dmg)) {
                        WARN(1, DM_GECKO_PREFIX "trim on free block!\n");
                } else {
                        mapped_v_blk = get_rmap_element(dmg, l_block);

                        BUG_ON(v_block != mapped_v_blk);

                        update_rmap(dmg, l_block, dmg->size);
                        dmg->d_map[v_block] = mark_block_free(dmg);

                        ++stats->discards;
                }
        }
        //spin_unlock_irqrestore(htable_lock, table_flags);
        spin_unlock_bh(htable_lock);
        /*
        if (freed_blocks > 0) {
                wake_up_free_space_available(dmg);
        }*/
        /* discards are not issued since we have HDDs not SSDs */
        bio_endio(bio);
        return DM_MAPIO_SUBMITTED;
}

//static int gecko_map(struct dm_target *ti, struct bio *bio,
//                     union map_info *map_context)
// for profiling
unsigned int FIRST = 1;
static int gecko_map(struct dm_target *ti, struct bio *bio)
{
        // profiling
        if (FIRST) {  // get the start time of the test
                ktime_get_ts64(&rw_clock[0]);
                FIRST = 0;
        }

        struct dm_gecko *dmg = (struct dm_gecko *)ti->private;
        int ret = DM_MAPIO_REQUEUE;

        down_read(&dmg->metadata_sync_sema);  // ziggy: rw_semaphore

#if LINUX_VERSION_CODE <= KERNEL_VERSION(2, 6, 36)
        if (bio_empty_barrier(bio)) {
#else
        if (op_is_flush(bio->bi_opf)) {
#endif
                struct dm_io_region where;
                struct dm_gecko_stats *stats;
#if LINUX_VERSION_CODE <= KERNEL_VERSION(2, 6, 35)
                unsigned target_req_nr = map_context->flush_request;
#else
                //unsigned target_req_nr = map_context->target_request_nr;
                // ziggy
                unsigned target_req_nr = dm_bio_get_target_bio_nr(bio);

                printk("ziggy: gecko_map. Get FLUSH request, the `target_req_nr` # is: %u.\n'", target_req_nr);

#endif
                get_cpu();
                stats = this_cpu_ptr(dmg->stats);
                ++stats->empty_barriers;
                put_cpu();
                /* TODO: fix the case when the dmg->head just advanced
                 * across disks rendering the barrier ineffective on
                 * the indended disks. When dmg->head linear index
                 * advances to a new disk, the previous disk's mirrors
                 * are to be put in a lower power state. Currently, we
                 * issue a synchronous barrier so that all in-progress
                 * writes will complete before continuing, also fixing
                 * the case. */
            if (dmg->disk_map.layout == raid0) {
                    linear_to_phy_which(dmg,
                            block_to_sector(dmg->head) +  // ziggy: in this version, "head" is the disk that handle the write requests.

                            target_req_nr,
                            0, &where);
            } else {
                    linear_to_phy_which(dmg, block_to_sector(dmg->head),
                            target_req_nr, &where);
            }
            bio->bi_disk = where.bdev->bd_disk;
            /* the empty barriers do not indicate which
            * sectors:size are sync'ed */

            ret = DM_MAPIO_REMAPPED;
            goto out;
        }

        DPRINTK("%s request for sector %llu, %u bytes",
                bio_op(bio) == WRITE ? "WRITE" :
                (bio_op(bio) == READ ? "READ" : "READA"),
                (unsigned long long)bio->bi_iter.bi_sector, bio->bi_iter.bi_size);

/****************************************************/
        if (bio_op(bio) == REQ_OP_DISCARD) {
                printk("ziggy: %s, in REQ_OP_DISCARD branch, before map_discard(), line %d\n", __func__, __LINE__);
                ret = map_discard(dmg, bio);
        } else {
                ret = map_rw(dmg, bio);
        }
out:
        up_read(&dmg->metadata_sync_sema);

        return ret;
}

static void dm_gecko_put_devices(struct dm_target *ti, struct dm_gecko *dmg)
{
        int sit_idx;
        struct dev_sit_entry *tmp_sit;
        struct list_head *dm_dev_segs = &dmg->disk_map.dm_dev_segs;

        while (!list_empty(dm_dev_segs)) {
                int i;
                struct dm_dev_seg *seg =
                    list_entry(dm_dev_segs->next, struct dm_dev_seg, list);

                list_del(&seg->list);
                for (i = 0; i < dmg->disk_map.stripes; i++) {
                        /* initializing the disk_map NULLifies
                         * ->dev[i] before dm_get_device */
                         if (seg->dev[i]) {
                                dm_put_device(ti, seg->dev[i]);
                         }
                }

                // free zone metadata
                kfree(seg->zones);

                // Dev(ziggy): free added segments related metadata.
                for ( sit_idx = 0; sit_idx < seg->total_seg_nr; sit_idx++) {
                        tmp_sit = seg->sit[sit_idx];
                        vfree(tmp_sit->r_map);  // free the segment r_map
                        kfree(tmp_sit);
                }
                vfree(seg->sit);
                //kfree(seg->sit);
                kfree(seg);
        }
}

/* Schedule power adjustment for all mirror stripes except for
 * ->head_seg or ->tail_seg if gc is independent from read.
 * WARNING: since the power adjustment happens asynchornously
 * on the workqueue, make sure the workqueue does not disappear
 * from right under.  */
static void sched_delayed_power_adjustment_for_segments(struct dm_gecko *dmg)
{
  struct dm_dev_seg *seg;
        if (test_bit(DM_GECKO_READ_TPUT, &dmg->flags)) {
                return;
        }
        list_for_each_entry(seg, &dmg->disk_map.dm_dev_segs, list) {
                sched_delayed_power_adjustment_for_segment(seg,
                                                           dmg->low_pow_state);
        }
}

// This function is synchronous.
static void power_up_segment(struct dm_dev_seg *seg) {
        struct dm_gecko *dmg = seg->ctxt;
        int i;
        for (i = 0; i < dmg->disk_map.stripes; i++) {
                set_drive_power(seg->dev[i]->bdev, active);
        }
}

// This function is synchronous.
static void power_up_all_segments(struct dm_gecko *dmg)
{
        struct dm_dev_seg *seg;
        list_for_each_entry(seg, &dmg->disk_map.dm_dev_segs, list) {
                power_up_segment(seg);
        }
}

static struct dm_dev_seg *seg_alloc_and_init(gfp_t flags, struct dm_gecko *dmg)
{
        struct dm_dev_seg *seg = kmalloc(sizeof(*seg), flags);
        int j;

        if (!seg) {
                return NULL;
        }
        seg->ctxt = dmg;
        seg->cur_pow_state = seg->next_pow_state = unspecified;
        seg->access_seq_in_log = 0;
        atomic_set(&seg->pending_writes, 0);
        atomic_set(&seg->pending_reads, 0);
        INIT_WORK(&seg->cleaning_work, start_gc_on_dev);
        INIT_WORK(&seg->work, run_dm_dev_seg);
        for (j = 0; j < dmg->disk_map.stripes; j++) {
                /* ensure error handing works */
                seg->dev[j] = NULL;
        }
        return seg;
}

static int load_dm_gecko(struct dm_target *ti, struct dm_gecko *dmg)
{
        struct dm_gecko_persistent_metadata *dmg_meta;
        struct dm_gecko_dev *dmg_devs;
        size_t i, map_size;
        int sz, err = 0, disk_cnt;
        struct file *file;
        loff_t pos = 0;

        char *page = (char *)__get_free_page(GFP_KERNEL);
        if (!page) {
                return -ENOMEM;
        }
        file = filp_open(dmg->meta_filename, O_LARGEFILE | O_RDONLY, 0);
        if (IS_ERR(file)) {
                printk(DM_GECKO_PREFIX "open %s\n", dmg->meta_filename);
                err = PTR_ERR(file);
                goto out;
        }

        // ziggy
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 14, 0)
        mm_segment_t old_fs = get_fs();
        set_fs(KERNEL_DS);

        sz = vfs_read(file, page, PAGE_SIZE, &pos);
#else
        sz = kernel_read(file, page, PAGE_SIZE, &pos);
#endif
        if (sz != PAGE_SIZE) {
                err = (sz < 0) ? sz : -EIO;
                printk(DM_GECKO_PREFIX "read metadata %s: %d\n",
                       dmg->meta_filename, err);
                goto out_close;
        }

        dmg_meta = (struct dm_gecko_persistent_metadata *)page;
        if (dmg_meta->magic != DM_GECKO_META_MAGIC) {
                printk(DM_GECKO_PREFIX "magic number error, endianness?\n");
                err = -EINVAL;
                goto out_close;
        }

        dmg->incarnation = dmg_meta->incarnation + 1;
        dmg->size = dmg_meta->size;
        dmg->tail = dmg_meta->tail;
        dmg->head = dmg_meta->head;
        //dmg->available_blocks = dmg_meta->available_blocks;
        atomic_set(&dmg->available_blocks, dmg_meta->available_blocks);
        //dmg->free_blocks = dmg_meta->free_blocks;
        atomic_set(&dmg->free_blocks, dmg_meta->free_blocks);
        dmg->disk_map.layout = dmg_meta->layout;
        dmg->disk_map.stripes = dmg_meta->stripes;
        BUG_ON(dmg->disk_map.stripes > DM_GECKO_MAX_STRIPES);
        dmg->flags = dmg_meta->flags;
	dmg->max_gc_req_in_progress = dmg_meta->max_gc_req_in_progress;
        dmg->gc_ctrl = dmg_meta->gc_ctrl;
        dmg->low_pow_state = dmg_meta->low_pow_state;

        disk_cnt = dmg_meta->disk_map_cnt * dmg_meta->stripes;
        if (disk_cnt * sizeof(*dmg_devs) + sizeof(*dmg_meta) > PAGE_SIZE) {
                printk(DM_GECKO_PREFIX "too many disks\n");
                err = -EINVAL;
                goto out_close;
        }

        dmg_devs = (struct dm_gecko_dev *) (page + sizeof(*dmg_meta));
        INIT_LIST_HEAD(&dmg->disk_map.dm_dev_segs);
        dmg->disk_map.cnt = 0;
        dmg->disk_map.len = 0;
        for (i = 0; i < dmg_meta->disk_map_cnt; i++) {
                int j;
                sector_t stripes_len = 0;
                struct dm_dev_seg *seg = seg_alloc_and_init(GFP_KERNEL, dmg);
                if (!seg) {
                        printk(DM_GECKO_PREFIX "kmalloc dm_dev_seg\n");
                        err = -ENOMEM;
                        goto out_err_1;
                }
                seg->idx = i;
                list_add_tail(&seg->list, &dmg->disk_map.dm_dev_segs);

                for (j = 0; j < dmg_meta->stripes; j++) {
                        struct dm_gecko_dev *dmg_dev =
                            &dmg_devs[i * dmg_meta->stripes + j];
                        err = dm_get_device(ti,
                                            dmg_dev->name,
                                            dm_table_get_mode(ti->table),
                                            &seg->dev[j]);
                        if (err) {
                                printk(DM_GECKO_PREFIX
                                       "device lookup failed\n");
                                goto out_err_1;
                        }
                        if (seg->dev[0]->bdev->bd_inode->i_size !=
                            seg->dev[j]->bdev->bd_inode->i_size) {
                                printk(DM_GECKO_PREFIX
                                       "stripes must match in size "
                                       "(%llu != %llu)\n",
                                       seg->dev[0]->bdev->bd_inode->i_size,
                                       seg->dev[j]->bdev->bd_inode->i_size);
                                err = -EINVAL;
                                goto out_err_1;
                        }
                        stripes_len += seg->dev[j]->bdev->bd_inode->i_size;
                }

                seg->start = dmg->disk_map.len;
                seg->len = (dmg->disk_map.layout == raid0) ?
                    (stripes_len >> SECTOR_SHIFT) :
                    (seg->dev[0]->bdev->bd_inode->i_size >> SECTOR_SHIFT);
                dmg->disk_map.len += seg->len;

                ++dmg->disk_map.cnt;
        }
        if (dmg->disk_map.len != ti->len) {
                printk(DM_GECKO_PREFIX
                       "disk_map length != dm_target length\n");
                err = -EINVAL;
                goto out_err_1;
        }
        BUG_ON(dmg->size != sector_to_block(dmg->disk_map.len));

        /* allocate the maps */
        map_size = PAGE_ALIGN(sizeof(*dmg->d_map) * dmg->size);
        dmg->d_map = vmalloc(map_size);
        if (!dmg->d_map) {
                printk(DM_GECKO_PREFIX "vmalloc ->d_map failed\n");
                err = -ENOMEM;
                goto out_err_1;
        }
        /* same size as direct map */
        /*
        dmg->r_map = vmalloc(map_size);
        if (!dmg->r_map) {
                printk(DM_GECKO_PREFIX "vmalloc ->r_map failed\n");
                err = -ENOMEM;
                goto out_err_2;
        }
        */

        /* read the maps (the maps are multiple of PAGE_SIZE for convenience) */
        for (i = 0; i < map_size; i += PAGE_SIZE) {
                char *dest = &((char *)dmg->d_map)[i];

                // ziggy
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 14, 0)
                sz = vfs_read(file, dest, PAGE_SIZE, &pos);
#else
                sz = kernel_read(file, dest, PAGE_SIZE, &pos);
#endif
                if (sz != PAGE_SIZE) {
                        err = (sz < 0) ? sz : -EIO;
                        printk(DM_GECKO_PREFIX "kernel_read ->d_map\n");
                        goto out_err_3;
                }
        }
        /*
        for (i = 0; i < map_size; i += PAGE_SIZE) {
                char *dest = &((char *)dmg->r_map)[i];

#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 14, 0)
                sz = vfs_read(file, dest, PAGE_SIZE, &pos);
#else
                sz = kernel_read(file, dest, PAGE_SIZE, &pos);
#endif
                if (sz != PAGE_SIZE) {
                        err = (sz < 0) ? sz : -EIO;
                        printk(DM_GECKO_PREFIX "vfs_read ->r_map\n");
                        goto out_err_3;
                }
        }
        */
out_close:
        filp_close(file, current->files);
out:
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 14, 0)
        set_fs(old_fs);
#endif
        free_page((unsigned long)page);
        return err;

out_err_1:
        dm_gecko_put_devices(ti, dmg);
        goto out_close;

out_err_2:
        kfree(dmg->d_map);
        goto out_err_1;

out_err_3:
        // TODO: we have not handle the segment r_map yet
        //kfree(dmg->r_map);
        goto out_err_2;
}

/*
void create_artificial_metadata_maps(struct dm_gecko *dmg,
				     sector_t total_blocks,
				     int data_blocks,
				     int free_blocks) {
        u32 vblock = 0;
	enum fake_writing {
	        FAKE_WR_BLOCKS,
		FAKE_WR_HOLES,
	};
	enum fake_writing state = FAKE_WR_BLOCKS;  // start by writing blocks
	int blocks_written = 0;
	int holes_written = 0;
	sector_t size = dmg->tail + total_blocks;
	BUG_ON(size > 0xffffffff);
	BUG_ON(size > dmg->size);
	set_bit(DM_GECKO_GC_FORCE_STOP, &dmg->flags);  // force-stop the gc
	for (dmg->head = dmg->tail; dmg->head < size; ++dmg->head) {
	        // The entire map contains `holes'.
	        BUG_ON(dmg->r_map[dmg->head] != mark_block_free(dmg));
		switch (state) {
		case FAKE_WR_BLOCKS:
		        dmg->r_map[dmg->head] = vblock;
			dmg->d_map[vblock] = dmg->head;
			++vblock;
			--dmg->persistent_available_blocks;
			--dmg->available_blocks;
			--dmg->free_blocks;
			if (++blocks_written >= data_blocks) {
			        holes_written = 0;
				state = FAKE_WR_HOLES;
			}
			break;
		case FAKE_WR_HOLES:
		        if (++holes_written >= free_blocks) {
			        blocks_written = 0;
				state = FAKE_WR_BLOCKS;
			}
			break;
		default:
		        printk(DM_GECKO_PREFIX
			       "invalid artificial metadata map write state\n");
			BUG_ON(1);
			break;
		}
	}
}*/

/*
 * insmod dm-gecko_mod.ko <persistent (true=1 | false)>
 * <metadata-file> [<layout ("linear" | "raid1" | "raid0")>]
 * [<# of stripes>] <# of devices> [<dev_path>]+ ]?
 *
 * Note that a "linear" layout is equivalent to a "raid0" or a "raid1"
 * layout with a single stripe (it exists for historical reasons---the
 * first one to be developed).
 *
 * If (!persistent) then the layout, number and device paths are
 * irrelevant.  Otherwise, the metadata is persistently saved when the
 * target it destroyed.  The metadata should also be synchronized to
 * persistent storage periodically and perhaps only dirty mappings
 * should be updated (i.e. as results of reads). */
// ziggy: Construct a mapped device.
static int gecko_ctr(struct dm_target *ti, unsigned int argc, char *argv[])
{
        int err = -ENOMEM, persistent, i, dm_devs, arg = 0;
        char *end;
	u32 mapidx;
        struct dev_sit_entry *tmp;
        struct dm_dev_seg *tmp_dev_itr;
        int k;

        struct dm_gecko *dmg = kmalloc(sizeof(*dmg), GFP_KERNEL);
        if (!dmg) {
                ti->error = DM_GECKO_PREFIX "unable to allocate gecko context";
                goto out1;
        }
        memset(dmg, 0, sizeof(*dmg));        /* zeros out the stats as well */
        // TODO: agree on a set of default startup flags
        set_bit(DM_GECKO_READ_TPUT, &dmg->flags);
        spin_lock_init(&dmg->lock);
        //spin_lock_init(&dmg->htable_lock); /* ziggy */
        spin_lock_init(&dmg->write_dev_lock);  /* ziggy */
        init_waitqueue_head(&dmg->jobs_pending_waitqueue);
        init_waitqueue_head(&dmg->no_free_space_waitqueue);
        init_waitqueue_head(&dmg->pending_write_waitqueue);
        init_waitqueue_head(&dmg->kcopyd_job_waitqueue);
        init_waitqueue_head(&dmg->gc_waitqueue);
        init_waitqueue_head(&dmg->gc_pending_read_waitqueue);
        init_rwsem(&dmg->metadata_sync_sema);
        atomic_set(&dmg->total_jobs, 0);
        atomic_set(&dmg->htable_size, 0);
        atomic_set(&dmg->nr_kcopyd_jobs, 0);
        atomic_set(&dmg->gc_wait_for_req, 0);
        // TODO(ziggy): Build new cleaning mechanism
        //hrtimer_init(&dmg->timer, CLOCK_MONOTONIC, HRTIMER_MODE_REL);
        //dmg->timer.function = fire_gc_timer;
        //INIT_WORK(&dmg->gc_work, try_sched_gc);  // ziggy: work, func. It will be put into gecoko_wqueue
        //atomic_set(&dmg->gc_work_scheduled_by_timer, 0);
        //atomic_set(&dmg->timer_active, 1);
        //INIT_WORK(&dmg->sync_metadata_work, sync_metadata);
        //dmg->gc_req_in_progress = 0;
        dmg->max_gc_req_in_progress = GC_CONCURRENT_REQ;
        dmg->low_pow_state = DEFAULT_LOW_POW_STATE;
        dmg->incarnation = 1;
        // Dev (ziggy)
        //dmg->total_free_seg_nr = 0;

        if (!(dmg->stats = alloc_percpu(struct dm_gecko_stats))) {
                ti->error = DM_GECKO_PREFIX "unable to alloc_percpu stats";
                printk("%s\n", ti->error);
                goto free_dmg;
        }

        err = dmg_kcopyd_client_create(DM_GECKO_GC_COPY_PAGES,  // ziggy: initialization
                                       &dmg->kcopyd_client);

        if (err) {
                ti->error =
                    DM_GECKO_PREFIX "unable to register as a kcopyd client";
                printk("%s\n", ti->error);
                goto free_dmg_stats;
        }

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 0, 0)
        dmg->io_client = dm_io_client_create();
#else
        dmg->io_client = dm_io_client_create(DM_GECKO_GC_COPY_PAGES);
#endif
        if (IS_ERR(dmg->io_client)) {
                ti->error =
                    DM_GECKO_PREFIX "unable to register as an io client";
                err = PTR_ERR(dmg->io_client);
                printk("%s, errno=%d\n", ti->error, err);
                goto out4;
        }

        /* parse args */

        /* ziggy: parameter parsing */
        persistent = simple_strtol(argv[arg++], &end, 10);  // convert a string to unsigned long long
        if (*end) {
                ti->error = DM_GECKO_PREFIX "invalid persistence arg";
                printk("%s\n", ti->error);
                err = -EINVAL;
                goto out5;
        }

        dmg->meta_filename = kstrdup(argv[arg++], GFP_KERNEL);
        if (!dmg->meta_filename) {
                ti->error = DM_GECKO_PREFIX "unable to kstrdup meta-filename";
                printk("%s\n", ti->error);
                err = -ENOMEM;
                goto out5;
        }

        if (persistent) {
                err = load_dm_gecko(ti, dmg);  // ziggy: Read persisted metadata from file.
                if (err) {
                        ti->error =
                            DM_GECKO_PREFIX
                            "unable to load gecko from meta-file";
                        printk("%s\n", ti->error);
                        goto out5;
                }
                goto dm_maps_ready;
        }

        if (!persistent && argc < 5) {
                ti->error = DM_GECKO_PREFIX "insufficient arguments";
                printk("%s\n", ti->error);
                err = -EINVAL;
                goto out5;
        }

        if (strcmp(argv[arg], "linear") == 0) {
                dmg->disk_map.layout = linear;
        } else if (strcmp(argv[arg], "raid1") == 0) {
                dmg->disk_map.layout = raid1;
        } else if (strcmp(argv[arg], "raid0") == 0) {
                dmg->disk_map.layout = raid0;
        } else {
                ti->error = DM_GECKO_PREFIX "invalid layout";
                printk("%s\n", ti->error);
                err = -EINVAL;
                goto out5;
        }
        ++arg;
        
        if (dmg->disk_map.layout == raid1 || dmg->disk_map.layout == raid0) {
                dmg->disk_map.stripes = simple_strtoul(argv[arg++], &end, 10);
            if (*end || dmg->disk_map.stripes > DM_GECKO_MAX_STRIPES) {
                    ti->error = DM_GECKO_PREFIX "invalid number of stripes";
                printk("%s\n", ti->error);
                err = -EINVAL;
                goto out5;
            }
        } else {
                dmg->disk_map.stripes = 1;
        }
        printk(DM_GECKO_PREFIX "# of stripes: %d\n", dmg->disk_map.stripes);
        dm_devs = simple_strtoul(argv[arg++], &end, 10);
        if (!(*end)) {
        printk(DM_GECKO_PREFIX "# of devices: %d\n", dm_devs);
        }

        if (!dm_devs || *end || dm_devs != (argc - arg) ||
            ((dmg->disk_map.layout == raid1 || dmg->disk_map.layout == raid0) &&
            (dm_devs % dmg->disk_map.stripes != 0))) {
                ti->error = DM_GECKO_PREFIX "invalid number of devices";
                printk("%s\n", ti->error);
                err = -EINVAL;
                goto out5;
        }

        // ziggy: commence on initialization
        INIT_LIST_HEAD(&dmg->disk_map.dm_dev_segs);
        dmg->disk_map.cnt = dm_devs / dmg->disk_map.stripes;  // ziggy: TODO is this right? The @cnt can be zero.
        dmg->disk_map.len = 0;
        dmg->writing_dev = 0;
        for (i = 0; i < dmg->disk_map.cnt; i++) {
                int j;
                sector_t stripes_len = 0;

                struct dm_dev_seg *seg = seg_alloc_and_init(GFP_KERNEL, dmg);  // ziggy: #seg == #devices sets
                if (!seg) {
                        ti->error = DM_GECKO_PREFIX "kmalloc dm_dev_seg";
                        printk("%s\n", ti->error);
                        err = -ENOMEM;
                        goto out6;
                }
                seg->idx = i;
                list_add_tail(&seg->list, &dmg->disk_map.dm_dev_segs);

                // ziggy: Add devices to the system list
                for (j = 0; j < dmg->disk_map.stripes; j++) {
                        err = dm_get_device(  // ziggy: Add a device into the dm list.
                            ti,  // dm_target
                            argv[arg + (i * dmg->disk_map.stripes + j)],  // device path, @i for seg, @j for stripes
                            dm_table_get_mode(ti->table),
                            &seg->dev[j]);  // the device is registered.
                        if (err) {
                                ti->error =
                                    DM_GECKO_PREFIX "device lookup failed";
                                printk("%s\n", ti->error);
                                goto out6;
                        }

                // TODO(tudorm): take the min size
            // here, make sure each device in the segment has the same size.
                        if (seg->dev[0]->bdev->bd_inode->i_size !=
                            seg->dev[j]->bdev->bd_inode->i_size) {  
                                ti->error =
                                    DM_GECKO_PREFIX
                                    "stripes must match in size";
                                printk("%s (%llu != %llu)\n", ti->error,
                                       seg->dev[0]->bdev->bd_inode->i_size,
                                       seg->dev[j]->bdev->bd_inode->i_size);
                                err = -EINVAL;
                                goto out6;
                        }
                        stripes_len += seg->dev[j]->bdev->bd_inode->i_size;
                        printk(DM_GECKO_PREFIX "added disk for stripe %d:%d\n",
                               i, j);
                }
                seg->start = dmg->disk_map.len;
                seg->len = (dmg->disk_map.layout == raid0) ?
                        (stripes_len >> SECTOR_SHIFT) :
                        (seg->dev[0]->bdev->bd_inode->i_size >> SECTOR_SHIFT);
                printk(DM_GECKO_PREFIX "sector %d start=%lld and len=%lld\n",
                        seg->idx, seg->start, seg->len);
                dmg->disk_map.len += seg->len;

                // Dev(ziggy): compute number of segments
                seg->total_seg_nr = (seg->len >> CLEAN_SECTOR_TO_SEGMENT);
                seg->free_seg_nr = seg->total_seg_nr - 1;  // the first one is set to the curr->seg.
                //dmg->total_free_seg_nr += seg->free_seg_nr;
                printk("ziggy: there are %d segments in this dev. \n", seg->total_seg_nr);

                seg->zones = kmalloc(sizeof(struct smr_zone) * (seg->total_seg_nr >> SEG_TO_ZONE_SHIFT), GFP_KERNEL);
                if (!seg->zones) {
                        ti->error = DM_GECKO_PREFIX "kmalloc -> seg->zones failed";
                        printk("%s\n", ti->error);
                        err = -ENOMEM;
                        goto out6;
                }
                for (k = 0; k < seg->total_seg_nr >> SEG_TO_ZONE_SHIFT; k++) {
                        seg->zones[k].zone_idx = k;
                        seg->zones[k].size = 1 << SEG_TO_ZONE_SHIFT;
                        atomic_set(&seg->zones[k].nr_invalid_blk, 0);
                }

                /* Dev(ziggy): SIT initialization. Also build free segments list */
                //seg->sit = kmalloc(sizeof(struct dev_sit_entry *) * seg->free_seg_nr, GFP_KERNEL);
                seg->sit = vmalloc(sizeof(struct dev_sit_entry *) * seg->free_seg_nr);
                if (!seg->sit) {
                        //ti->error = DM_GECKO_PREFIX "vmalloc -> seg->sit failed";
                        ti->error = DM_GECKO_PREFIX "kmalloc -> seg->sit failed";
                        printk("%s\n", ti->error);
                        err = -ENOMEM;
                        goto out6;
                }

                INIT_LIST_HEAD(&seg->free_list.list);

                for (k = 0; k < seg->total_seg_nr; ++k) {
                        tmp = (struct dev_sit_entry *)kmalloc(sizeof(struct dev_sit_entry), GFP_KERNEL);
                        tmp->seg_nr = k;
                        tmp->itr = 0;
                        tmp->temperature = 0;
                        //tmp->nr_invalid_blk = 0;
                        atomic_set(&tmp->nr_invalid_blk, 0);
                        /* allocate per segment r_map for each sigment */
                        //tmp->rmap = vmalloc(PAGE_ALIGN(sizeof(*tmp->r_map) * BLOCKS_PER_SEGMENT));
                        /* seems no need to align wiht page, this chunk is only 1kB */
                        tmp->r_map = vmalloc(sizeof(*tmp->r_map) * BLOCKS_PER_SEGMENT);
                        if (!tmp->r_map) {
                                ti->error = DM_GECKO_PREFIX "vmalloc ->r_map failed";
                                printk("%s\n", ti->error);
                                err = -ENOMEM;
                                goto out7;
                        }
                        seg->sit[k] = tmp;

                        if (k == 0) {  // first segment is the default free segment, so we do not add it to the free list.
                                seg->curr_seg = tmp;
                                continue;  // we will use the 0th segment in the 0th dev, so does not need to add it into free list
                        }

                        list_add_tail(&tmp->list, &seg->free_list.list);
                }
        }
        //printk("ziggy DEBUG: WHOLE SYSTEM total_free_seg_nr = %d\n", dmg->total_free_seg_nr);

        if (dmg->disk_map.len != ti->len) {
                ti->error =
                    DM_GECKO_PREFIX "disk_map length != dm_target length";
                printk("%s\n", ti->error);
                err = -EINVAL;
                goto out6;
        }

        if (sector_to_block(dmg->disk_map.len) > 0xffffffff-1) {
                ti->error = DM_GECKO_PREFIX "unsupported size (too large)";
                    printk("%s \n", ti->error);
                    err = -EINVAL;
                    goto out6;
        }

        /* TODO: need to round to minimal block numbers */
        dmg->size = sector_to_block(dmg->disk_map.len);
        /* (dmg->size-1) for circular buffer logic: one slot wasted to
         * distinguish between full and empty circular buffer. */
        //dmg->available_blocks = dmg->free_blocks = dmg->size-1;
        atomic_set(&dmg->available_blocks, dmg->size-1);
        atomic_set(&dmg->free_blocks, dmg->size-1);
        /*
        dmg->gc_ctrl.low_watermark = GC_DEFAULT_LOW_WATERMARK;
        dmg->gc_ctrl.high_watermark = GC_DEFAULT_HIGH_WATERMARK;
        */
        // ziggy: Fix
        dmg->gc_ctrl.low_watermark = dmg->size * 10 / 100;
        dmg->gc_ctrl.high_watermark = dmg->size * 15 / 100;

        /* Allocate the maps, initialize them, and also initialize the
         * circular pointers. The maps are page aligned and their size
         * is also a multiple of PAGE_SIZE to simplify the potentially
         * selective writing of the metadata. */
        dmg->d_map = vmalloc(PAGE_ALIGN(sizeof(*dmg->d_map) * dmg->size));  // ziggy: page size is 4K
        //dmg->d_map = kmalloc(PAGE_ALIGN(sizeof(*dmg->d_map) * dmg->size), GFP_KERNEL);  // ziggy: page size is 4K
        if (!dmg->d_map) {
                ti->error = DM_GECKO_PREFIX "vmalloc ->d_map failed";
                //ti->error = DM_GECKO_PREFIX "kmalloc ->d_map failed";
                printk("%s\n", ti->error);
                err = -ENOMEM;
                goto out6;
        }
        /*
        dmg->r_map = vmalloc(PAGE_ALIGN(sizeof(*dmg->r_map) * dmg->size));
        //dmg->r_map = kmalloc(PAGE_ALIGN(sizeof(*dmg->d_map) * dmg->size), GFP_KERNEL);  // ziggy: page size is 4K
        if (!dmg->r_map) {
                ti->error = DM_GECKO_PREFIX "vmalloc ->r_map failed";
                //ti->error = DM_GECKO_PREFIX "kmalloc ->r_map failed";
                printk("%s\n", ti->error);
                err = -ENOMEM;
                goto out7;
        }
        */
        /* Have to do the initializaiton here because the dmg->size is assigned after
         * we add all disk. */
        for (mapidx = 0; mapidx < dmg->size; ++mapidx) {
                dmg->d_map[mapidx] = mark_block_free(dmg);
        }
        /* initial per segment r_map */
        list_for_each_entry(tmp_dev_itr, &dmg->disk_map.dm_dev_segs, list) {
                int mapidx2;
                for (mapidx = 0; mapidx < tmp_dev_itr->total_seg_nr; mapidx++) {
                        for (mapidx2 = 0; mapidx2 < BLOCKS_PER_SEGMENT; mapidx2++) {
                                tmp_dev_itr->sit[mapidx]->r_map[mapidx2] = mark_block_free(dmg);
                        }

                }
        }

        dmg->tail = dmg->head = 0;
        // Write at 120MB/s for some time and spill into next disk
        // breaks the VM loop devices since they are fairly small

        /*
#define CLOSE_TO_MAX_SIZE_OFFSET (120 * 120 * (1024 * 1024 / GECKO_BLOCK_SIZE))
        if (dmg->size < CLOSE_TO_MAX_SIZE_OFFSET) {
                printk(DM_GECKO_PREFIX "WARNING, discarding overflow math\n");
        } else {
                dmg->tail = dmg->head = dmg->size - CLOSE_TO_MAX_SIZE_OFFSET;
        }
        */
        /*
	create_artificial_metadata_maps(dmg, dmg->size/2 + (10 * 1024),
	                                //dmg->size/16, 1);
                                        256, 256);
	*/
dm_maps_ready:

        /* alloc the htable of IO requests in-progress */
        dmg->buckets =
            kmalloc(sizeof(struct list_head) * HASH_TABLE_SIZE, GFP_KERNEL);
        if (!dmg->buckets) {
                ti->error = DM_GECKO_PREFIX "kmalloc htable failed";
                printk("%s\n", ti->error);
                err = -ENOMEM;
                goto out8;
        }

        for (i = 0; i < HASH_TABLE_SIZE; i++)
                INIT_LIST_HEAD(&dmg->buckets[i]);
        //dmg->htable_size = 0;        /* rendered redundant by memset */
        dmg->head_seg = sector_to_dev(dmg, block_to_sector(dmg->head));
        ++dmg->head_seg->access_seq_in_log;
        dmg->tail_seg = sector_to_dev(dmg, block_to_sector(dmg->tail));

        /* alloc the htable lock */
        dmg->htable_entry_locks =
            kmalloc(sizeof(spinlock_t) * HASH_TABLE_SIZE, GFP_KERNEL);
        if (!dmg->htable_entry_locks) {
                ti->error = DM_GECKO_PREFIX "kmalloc htable locks failed";
                printk("%s\n", ti->error);
                err = -ENOMEM;
                goto out9;
        }

        for (i = 0; i < HASH_TABLE_SIZE; i++)
                spin_lock_init(&dmg->htable_entry_locks[i]);

        // ziggy
        // ti->split_io = GECKO_SECTORS_PER_BLOCK;  /* size in # of sectors Has been deleted */
        // ti->num_flush_requests = dmg->disk_map.stripes;  // ziggy: this field is gone
        ti->max_io_len = GECKO_SECTORS_PER_BLOCK;  // ziggy: from dm-tx
        ti->num_flush_bios = dmg->disk_map.stripes;  // ziggy: TODO not sure whether we should use stripes or seg(cnt).
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 36)
        //ti->num_discard_requests = dmg->disk_map.stripes;
        ti->num_discard_bios = dmg->disk_map.stripes;
#endif
        ti->private = dmg;
        // sched_delayed_power_adjustment_for_segments(dmg);  // ziggy: this is not useful in our research

        // start timer right before returning
        // ziggy: stop the timer temporarily
        //dmg->timer_delay = ktime_set(GECKO_TIMER_PERIOD_SECS, GECKO_TIMER_PERIOD_NSECS);
        /*
        if ((err = hrtimer_start(&dmg->timer,
                                 dmg->timer_delay,
                                 HRTIMER_MODE_REL)) != 0) {
          ti->error = DM_GECKO_PREFIX "hrtimer_start failed";
          printk("%s\n", ti->error);
          goto out8;
        }
        */
        // ziggy
        //hrtimer_start(&dmg->timer, dmg->timer_delay, HRTIMER_MODE_REL);

        printk(DM_GECKO_PREFIX "gecko_ctr done (dm_gecko incarnation %llu).\n",
               dmg->incarnation);
        return 0;

out9:
        kfree(dmg->buckets);
out8:
        // TODO: we need to free r_map in the segment now
        //vfree(dmg->r_map);
out7:
        vfree(dmg->d_map);
out6:
        dm_gecko_put_devices(ti, dmg);
out5:
        dm_io_client_destroy(dmg->io_client);
out4:
        dmg_kcopyd_client_destroy(dmg->kcopyd_client);
free_dmg_stats:
        free_percpu(dmg->stats);
free_dmg:
        kfree(dmg);
out1:
        return err;
}

static long (*sys_rename_wrapper)(const char __user *oldname,
                                  const char __user *newname) = NULL;

/* store gecko metadata persistently */
static int store_dm_gecko(struct dm_gecko *dmg)
{
        struct dm_gecko_persistent_metadata* dmg_meta;
        struct dm_gecko_dev *dmg_devs;
        struct dm_dev_seg *seg;
        size_t i, map_size;
        int sz, err = 0;
        u32 rand_bytes;
        struct file *file;
        loff_t pos = 0;
        char *meta_filename_tmp, *page;
        char *dmg_devs_offset;
        mm_segment_t old_fs = get_fs();

        page = (char *)__get_free_page(GFP_KERNEL);
        if (!page)
                return -ENOMEM;

        if (sys_rename_wrapper != NULL) {
                // the temp filename consists of the original filename
                // concatenated with the hex value of sizeof(rand_bytes)
                // random bytes (a nibble is represented by one character).
                meta_filename_tmp = kmalloc(strlen(dmg->meta_filename) + 1 +
                        sizeof(rand_bytes) * 2, GFP_KERNEL);
                if (!meta_filename_tmp) {
                        err = -ENOMEM;
                        goto out_free_page;
                }
                get_random_bytes(&rand_bytes, sizeof(rand_bytes));
                sprintf(meta_filename_tmp, "%s%x",
                        dmg->meta_filename, rand_bytes);
        } else {
                meta_filename_tmp = dmg->meta_filename;
        }
        set_fs(KERNEL_DS);

        file = filp_open(meta_filename_tmp,
                         O_LARGEFILE | O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (!file) {
                printk(DM_GECKO_PREFIX "open %s\n", dmg->meta_filename);
                err = -EIO;
                goto out;
        }

        dmg_meta = (struct dm_gecko_persistent_metadata *)page;

        dmg_meta->incarnation = dmg->incarnation;
        dmg_meta->magic = DM_GECKO_META_MAGIC;
        dmg_meta->size = dmg->size;
        dmg_meta->tail = dmg->tail;
        dmg_meta->head = dmg->head;
        dmg_meta->available_blocks = atomic_read(&dmg->available_blocks);
        dmg_meta->free_blocks = atomic_read(&dmg->free_blocks);
        dmg_meta->layout = dmg->disk_map.layout;
        dmg_meta->stripes = dmg->disk_map.stripes;
        dmg_meta->disk_map_cnt = dmg->disk_map.cnt;
        dmg_meta->flags = dmg->flags;
	dmg_meta->max_gc_req_in_progress = dmg->max_gc_req_in_progress;
        // Clear the volatile flags.
        clear_bit(DM_GECKO_GC_FORCE_STOP, &dmg_meta->flags);
        clear_bit(DM_GECKO_FINAL_SYNC_METADATA, &dmg_meta->flags);
        clear_bit(DM_GECKO_SYNCING_METADATA, &dmg_meta->flags);
        dmg_meta->gc_ctrl = dmg->gc_ctrl;
        dmg_meta->low_pow_state = dmg->low_pow_state;

        dmg_devs_offset = page + sizeof(*dmg_meta);
        dmg_devs = (struct dm_gecko_dev *)dmg_devs_offset;
        if (dmg_meta->disk_map_cnt * dmg_meta->stripes * sizeof(*dmg_devs) >
            PAGE_SIZE - sizeof(*dmg_meta)) {
                printk(DM_GECKO_PREFIX "metadata too large (too many disks)\n");
                err = -EINVAL;
                goto out_close;
        }

        /* populate the disk map w/ disk device names */
        list_for_each_entry(seg, &dmg->disk_map.dm_dev_segs, list) {
                int i;
                for (i = 0; i < dmg->disk_map.stripes; i++) {
                        dev_t _dev;

                        BUG_ON(seg->dev[i] == NULL);

                        _dev = seg->dev[i]->bdev->bd_dev;
                        sprintf(dmg_devs->name, "%u:%u", MAJOR(_dev),
                                MINOR(_dev));
                        ++dmg_devs;
                }
        }

        BUG_ON(((unsigned long)dmg_devs) - ((unsigned long)dmg_meta)
               > PAGE_SIZE);
        /* Write PAGE_SIZE worth of data, to align subsequent maps */
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 14, 0)
        sz = vfs_write(file, page, PAGE_SIZE, &pos);
#else
        sz = kernel_write(file, page, PAGE_SIZE, &pos);
#endif
        if (sz != PAGE_SIZE) {
                err = (sz < 0) ? sz : -EIO;
                printk(DM_GECKO_PREFIX "vfs_write metadata, dev-map %d\n", err);
                goto out_close;
        }

        /* Write the maps, both maps have the same size, further, the
         * allocated (but possibly unused) size of the maps is a
         * multiple of PAGE_SIZE to make potentially selective
         * metadata writing easier (and more efficient). */
        map_size = PAGE_ALIGN(sizeof(*dmg->d_map) * dmg->size);
        for (i = 0; i < map_size; i += PAGE_SIZE) {
                char *src = &((char *)dmg->d_map)[i];

#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 14, 0)
                sz = vfs_write(file, src, PAGE_SIZE, &pos);
#else
                sz = kernel_write(file, src, PAGE_SIZE, &pos);
#endif 
                if (sz != PAGE_SIZE) {
                        err = (sz < 0) ? sz : -EIO;
                        printk(DM_GECKO_PREFIX "vfs_write ->d_map\n");
                        goto out_close;
                }
        }

        /* TODO: need to match segment r_map
        for (i = 0; i < map_size; i += PAGE_SIZE) {
                char *src = &((char *)dmg->r_map)[i];

#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 14, 0)
                sz = vfs_write(file, src, PAGE_SIZE, &pos);
#else
                sz = kernel_write(file, src, PAGE_SIZE, &pos);
#endif
                if (sz != PAGE_SIZE) {
                        err = (sz < 0) ? sz : -EIO;
                        printk(DM_GECKO_PREFIX "vfs_write ->r_map\n");
                        goto out_close;
                }
        }
        */
out_close:
        filp_close(file, current->files);

        if (sys_rename_wrapper != NULL) {
                err = sys_rename_wrapper(meta_filename_tmp, dmg->meta_filename);
                if (err) {
                        printk(DM_GECKO_PREFIX "sys_rename: %d\n", err);
                        goto out;
                }
        }

out:
        if (sys_rename_wrapper != NULL) {
                kfree(meta_filename_tmp);
        }
out_free_page:
        free_page((unsigned long)page);
        set_fs(old_fs);
        return err;
}

static void gecko_dtr(struct dm_target *ti)
{
        // At this point, `dmsetup message' cannot be issued against
        // the module any longer, therefore only the extant
        // metadata-sync and gc may be running (besides regular IOs
        // that have not yet completed).
        struct dm_gecko *dmg = (struct dm_gecko *)ti->private;

        // Wait for pending metadata sync to complete.
        down_write(&dmg->metadata_sync_sema);
        // Never clear this bit, the module is about to be unloaded.
        set_bit(DM_GECKO_FINAL_SYNC_METADATA, &dmg->flags);
        up_write(&dmg->metadata_sync_sema);

        set_bit(DM_GECKO_GC_FORCE_STOP, &dmg->flags);
        // Metadata sync may have restarted the timer upon exit.
        printk(DM_GECKO_PREFIX "hrtimer destroyed\n");
        atomic_set(&dmg->timer_active, 0);
        // ziggy: temperarely disable the timer
        //hrtimer_cancel(&dmg->timer);
        // Wait for pending IOs to complete.

        wait_event(dmg->jobs_pending_waitqueue, !atomic_read(&dmg->total_jobs));
        dmg_kcopyd_client_destroy(dmg->kcopyd_client);
        dm_io_client_destroy(dmg->io_client);
        // TODO(ziggy): we need to modify this function to store added metadata.
        //store_dm_gecko(dmg);
        // WARNING, must be done before put_devices.
        /*  we do not need power control
        power_up_all_segments(dmg);
        */
        // ziggy: also free added segment related resources
        dm_gecko_put_devices(ti, dmg);

        vfree(dmg->d_map);
        //vfree(dmg->r_map);
        kfree(dmg->buckets);
        free_percpu(dmg->stats);
        kfree(dmg->meta_filename);
        kfree(dmg);
        printk(DM_GECKO_PREFIX "gecko_dtr done.\n");

        // profiling
        total_gc_time += gc_running_time;
        printk("ziggy: total rw time is %lld, rw # %lld\n ", total_rw_time, total_rw_count);
        printk("ziggy: total GC time is %lld, GC # %lld\n", total_gc_time , gc_running_count);
        printk("ziggy: locking time:\n");
        printk("\t rw_dmglock_time: %lld, cnt: %lld\n", rw_dmglock_time, rw_dmglock_count);
        printk("\t dev_gc_tar_time: %lld, cnt: %lld\n", dev_gc_tar_time, dev_gc_tar_cnt);
        printk("\t dispatch_dmglock_time: %lld, cnt: %lld\n", dispatch_dmglock_time, dispatch_seg_cnt);
        printk("\t dest blk allocate time: %lld, cnt: %lld\n",  dest_blk_for_gc_time, dest_blk_for_gc_cnt);
        printk("\t gc_read_dmglock_time: %lld, cnt: %lld\n", gc_read_dmglock_time, gc_read_callback_cnt);
        printk("\t gc_complete_dmglock_time %lld, cnt: %lld\n", gc_complete_dmglock_time, gc_complete_callback_cnt);
        printk("\t gc_complete_joblock_time %lld, cnt: %lld\n", gc_complete_joblock_time, gc_complete_joblock_cnt);
}

// static int gecko_status(struct dm_target *ti, status_type_t type,
//                        char *result, unsigned int maxlen)
// ziggy - TODO: the function interface does not match Linux 5's
// There is a new param "status_flags"
static void gecko_status(struct dm_target *ti, status_type_t type,
                unsigned status_flags, char *result, unsigned int maxlen)
{
        struct dm_gecko *dmg = (struct dm_gecko *)ti->private;
        int cpu, sz = 0;        /* sz is used by DMEMIT */
        struct dm_gecko_stats aggregate_stats, *cursor;
        struct deferred_stats aggregate_def_stats, *def_stats;

        memset(&aggregate_stats, 0, sizeof(aggregate_stats));
        memset(&aggregate_def_stats, 0, sizeof(aggregate_def_stats));

        for_each_possible_cpu(cpu) {
                cursor = per_cpu_ptr(dmg->stats, cpu);

                aggregate_stats.reads += cursor->reads;
                aggregate_stats.writes += cursor->writes;
                aggregate_stats.subblock_reads += cursor->subblock_reads;
                aggregate_stats.subblock_writes += cursor->subblock_writes;
                aggregate_stats.gc += cursor->gc;
                aggregate_stats.discards += cursor->discards;
                aggregate_stats.dropped_discards += cursor->dropped_discards;
                aggregate_stats.empty_barriers += cursor->empty_barriers;
                aggregate_stats.gc_recycle += cursor->gc_recycle;
                aggregate_stats.rw_clash += cursor->rw_clash;
                aggregate_stats.rw_gc_clash += cursor->rw_gc_clash;
                aggregate_stats.gc_clash += cursor->gc_clash;
                aggregate_stats.gc_rw_clash += cursor->gc_rw_clash;
                aggregate_stats.ww_clash += cursor->ww_clash;
                aggregate_stats.read_empty += cursor->read_empty;
                aggregate_stats.read_err += cursor->read_err;
                aggregate_stats.write_err += cursor->write_err;
                aggregate_stats.kcopyd_err += cursor->kcopyd_err;
                aggregate_stats.sb_read += cursor->sb_read;
                aggregate_stats.sb_write += cursor->sb_write;

                def_stats = &per_cpu(deferred_stats, cpu);
                aggregate_def_stats.gc += def_stats->gc;
                aggregate_def_stats.rw += def_stats->rw;
                aggregate_def_stats.total += def_stats->total;
        }

        switch (type) {
        case STATUSTYPE_INFO:
                DMEMIT("reads(%llu), writes(%llu), "
                       "subblock_reads(%llu), subblock_writes(%llu), "
                       "gc(%llu), discards(%llu), dropped_discards(%llu), "
                       "empty_barriers(%llu), "
                       "gc_recycle(%llu), rw_clash(%llu), rw_gc_clash(%llu), "
                       "gc_clash(%llu), gc_rw_clash(%llu), ww_clash(%llu), "
                       "read_empty (%llu), read_err(%llu), write_err(%llu), "
                       "kcopyd_err(%llu), sb_read(%llu), sb_write(%llu), "
                       "deferred_gc(%llu) deferred_rw(%llu), "
                       "deferred_total(%llu), total_jobs(%d)",
                       aggregate_stats.reads,
                       aggregate_stats.writes,
                       aggregate_stats.subblock_reads,
                       aggregate_stats.subblock_writes,
                       aggregate_stats.gc,
                       aggregate_stats.discards,
                       aggregate_stats.dropped_discards,
                       aggregate_stats.empty_barriers,
                       aggregate_stats.gc_recycle,
                       aggregate_stats.rw_clash,
                       aggregate_stats.rw_gc_clash,
                       aggregate_stats.gc_clash,
                       aggregate_stats.gc_rw_clash,
                       aggregate_stats.ww_clash,
                       aggregate_stats.read_empty,
                       aggregate_stats.read_err,
                       aggregate_stats.write_err,
                       aggregate_stats.kcopyd_err,
                       aggregate_stats.sb_read,
                       aggregate_stats.sb_write,
                       aggregate_def_stats.gc,
                       aggregate_def_stats.rw,
                       aggregate_def_stats.total,
                       atomic_read(&dmg->total_jobs));
                break;
        case STATUSTYPE_TABLE:
                DMEMIT("mode(%s{%d} | %s | %s | %s | %s) size(%lu), "
                       "htable_size(%lu), "
                       "tail(%lu|%d), head(%lu|%d), available_blocks(%lu), "
                       "free_blocks(%lu), used_blocks(%lu), "
                       "unavailable_blocks(%lu), "
                       "relocatable_blocks(%lu), gc_req_in_progress(%lu), "
                       "tail_wrap_around(%lu), head_wrap_around(%lu)",
                       dmg->disk_map.layout == linear ? "linear" :
		       (dmg->disk_map.layout == raid1 ? "raid1" : 
			(dmg->disk_map.layout == raid0 ? "raid0" : "unknown")),
		       dmg->disk_map.stripes,
                       test_bit(DM_GECKO_GC_FORCE_STOP,
                                &dmg->flags) ? "gc-off"
                       : (test_bit(DM_GECKO_GC_STARTED, &dmg->flags) ?
                          "gc-on" : "gc-idle"),
                       test_bit(DM_GECKO_READ_TPUT,
                                &dmg->flags) ? "max-read-tput" : "low-power",
                       test_bit(DM_GECKO_INDEPENDENT_GC,
                                &dmg->flags) ? "gc-independent" : "gc-random",
                       test_bit(DM_GECKO_SYNCING_METADATA,
                                &dmg->flags) ? "SYNC-METADATA-ON"
		       : "SYNC-METADATA-OFF",
                       (long unsigned)dmg->size,
                       (long unsigned)dmg->htable_size.counter,
                       (long unsigned)dmg->tail, dmg->tail_seg->idx,
                       (long unsigned)dmg->head, dmg->head_seg->idx,
                       (long unsigned)atomic_read(&dmg->available_blocks),
                       (long unsigned)atomic_read(&dmg->free_blocks),
                       (long unsigned)__used_blocks(dmg),
                       (long unsigned)__unavailable_blocks(dmg),
                       (long unsigned)__relocatable_blocks(dmg),
                       (long unsigned)dmg->gc_wait_for_req.counter,
                       dmg->tail_wrap_around,
                       dmg->head_wrap_around);
                if (test_bit(DM_GECKO_STATUS_DETAILED, &dmg->flags)) {
#define DMG_STATE0 0
#define DMG_STATE1 1
                        u32 tail, loopcnt, cursor, next_free, total_free;
                        u32 automata_state = DMG_STATE0;

                        DMEMIT("\n" DM_GECKO_PREFIX
                               "detail: tail=%lu, head=%lu\n",
                               (long unsigned)dmg->tail,
                               (long unsigned)dmg->head);

                        tail = dmg->tail;
                        cursor = dmg->tail;
                        next_free = dmg->tail;
                        total_free = 0;
                        u32 vblk_cursor = get_rmap_element(dmg, cursor);
                        for (loopcnt = 0; loopcnt < MAX_DETAIL_LOG_LOOP_CNT;) {

                                if (cursor == dmg->head)
                                        break;

                                switch (automata_state) {
                                case DMG_STATE0:
                                        //if (is_block_marked_free(dmg->r_map[cursor], dmg)) {
                                        if (is_block_marked_free(vblk_cursor, dmg)) {
                                                next_free = cursor;
                                                total_free = 1;
                                                automata_state = DMG_STATE1;
                                        }
                                        break;
                                case DMG_STATE1:
                                        //if (!is_block_marked_free(dmg->r_map[cursor], dmg)) {
                                        if (!is_block_marked_free(vblk_cursor, dmg)) {
                                                DMEMIT("%lu:%lu\n",
                                                       (long unsigned)
                                                       next_free,
                                                       (long unsigned)
                                                       total_free);
                                                ++loopcnt;
                                                total_free = 0;
                                                automata_state = DMG_STATE0;
                                        }
                                        break;
                                default:
                                        BUG_ON(1);
                                }

                                if ((++cursor) == dmg->size) {
                                        /* wrap around */
                                        cursor = 0;
                                }
                        }
                }
                break;
        }
}

// static int gecko_message(struct dm_target *ti, unsigned argc, char **argv)
static int gecko_message(struct dm_target *ti, unsigned argc, char **argv,
        char *result, unsigned maxlen)
{
        struct dm_gecko *dmg = (struct dm_gecko *)ti->private;

        if (argc < 1 || argc > 3) {
                ti->error = DM_GECKO_PREFIX "invalid number of arguments";
                goto bad;
        }

        if (strcmp(argv[0], "set-low-power") == 0) {
                if (argc == 2) {
                        if (strcmp(argv[1], "sleep") == 0) {
                                dmg->low_pow_state = sleep;
                        } else if (strcmp(argv[1], "standby") == 0) {
                                dmg->low_pow_state = standby;
                        } else {
                                printk(DM_GECKO_PREFIX
                                       "invalid set-low-power parameter: %s\n",
                                       argv[1]);
                                goto bad;
                        }
                }
                clear_bit(DM_GECKO_READ_TPUT, &dmg->flags);
                //sched_delayed_power_adjustment_for_segments(dmg);
        } else if (strcmp(argv[0], "set-high-read-tput") == 0) {
                set_bit(DM_GECKO_READ_TPUT, &dmg->flags);
                /* Need not need to power-up the mirrored disks
                 * explicitly, the Linux IDE driver is supposed to
                 * issue the reset lazyly and on demand. Do it anyway. */
                 //power_up_all_segments(dmg);
        } else if (strcmp(argv[0], "gc-independent") == 0) {
                set_bit(DM_GECKO_INDEPENDENT_GC, &dmg->flags);
                //power_up_segment(sector_to_dev(dmg, block_to_sector(dmg->tail)));
        } else if (strcmp(argv[0], "gc-off") == 0) {
                set_bit(DM_GECKO_GC_FORCE_STOP, &dmg->flags);
                clear_bit(DM_GECKO_GC_STARTED, &dmg->flags);
        } else if (strcmp(argv[0], "gc-on") == 0) {
                clear_bit(DM_GECKO_GC_FORCE_STOP, &dmg->flags);
                set_bit(DM_GECKO_GC_STARTED, &dmg->flags);
        } else if (strcmp(argv[0], "detail-on") == 0) {
                set_bit(DM_GECKO_STATUS_DETAILED, &dmg->flags);
        } else if (strcmp(argv[0], "detail-off") == 0) {
                clear_bit(DM_GECKO_STATUS_DETAILED, &dmg->flags);
        } else if (strcmp(argv[0], "sync-metadata") == 0) {
                do_sync_metadata(dmg);
        } else if (strcmp(argv[0], "sync-metadata-asynchronously") == 0) {
                queue_work(gecko_sync_metadata_wqueue,
                           &dmg->sync_metadata_work);
	} else if (strcmp(argv[0], "set-gc-max-concurrent-requests") == 0) {
	        int max_gc_concurrent_req;
		if (argc < 2) {
		        ti->error =
			  DM_GECKO_PREFIX "too few args (need one integer)";
			goto bad;
		}
		max_gc_concurrent_req = simple_strtol(argv[1], NULL, 10);
		if (max_gc_concurrent_req < MIN_GC_CONCURRENT_REQ ||
		    max_gc_concurrent_req > MAX_GC_CONCURRENT_REQ) {
		        ti->error =
			  DM_GECKO_PREFIX "invalid argument (not in range)";
			goto bad;
		}
		dmg->max_gc_req_in_progress = max_gc_concurrent_req;
        } else if (strcmp(argv[0], "set-gc-watermarks") == 0) {
                unsigned long low_gc_watermark, high_gc_watermark;
                if (argc < 3) {
                        ti->error =
			  DM_GECKO_PREFIX "too few args (need 2 watermarks)";
                        goto bad;
                }
                low_gc_watermark = simple_strtoul(argv[1], NULL, 10);
                high_gc_watermark = simple_strtoul(argv[2], NULL, 10);
                if (low_gc_watermark >= high_gc_watermark) {
                        ti->error =
                            DM_GECKO_PREFIX "low watermark >= high watermark";
                        goto bad;
                }
                dmg->gc_ctrl.low_watermark = low_gc_watermark;
                dmg->gc_ctrl.high_watermark = high_gc_watermark;
        } else {
                ti->error = DM_GECKO_PREFIX "invalid dmsetup message";
                goto bad;
        }

        return 0;
bad:
        printk("%s\n", ti->error);
        return -EINVAL;
}

// ziggy: Add new target type so dmsetup can use gecko
static struct target_type gecko_target = {
        .name = "gecko",
        .version = {1, 0, 1},
        .module = THIS_MODULE,
        .ctr = gecko_ctr,
        .dtr = gecko_dtr,
        .map = gecko_map,
        .status = gecko_status,
        .message = gecko_message,
};

static int __init dm_gecko_init(void)
{
#if ZIGGY_DEBUG
    // ziggy: DEBUG
    printk("ziggy: GECKO_BLOCK_SHIFT value - %d\n", GECKO_BLOCK_SHIFT );
    printk("ziggy: GECKO_BLOCK_SIZE value - %ld\n", GECKO_BLOCK_SIZE );

        // ziggy: TEST for insmod
        printk("%s, start!\n", __func__);

#endif

        int err = -ENOMEM;
#ifdef CONFIG_KALLSYMS
        unsigned long sys_rename_addr = kallsyms_lookup_name("sys_rename");
        if (sys_rename_addr == 0) {
                printk(DM_GECKO_PREFIX "Unable to lookup sys_rename symbol\n");
        } else {
                sys_rename_wrapper = (void *) sys_rename_addr;
                printk(DM_GECKO_PREFIX "Found sys_rename at address 0x%p\n",
                       sys_rename_wrapper);
        }
#elif defined SYS_RENAME_EXPORTED_TO_MODULES
        sys_rename_wrapper = sys_rename;
#endif

        /* init global resources for all gecko targets at module load
         * time */
        if ((err = dmg_kcopyd_init()) != 0) {
                printk(DM_GECKO_PREFIX "Unable to init kcopyd\n");
                goto out1;
        }
        if (!(io_for_block_cache = KMEM_CACHE(io_for_block, 0))) {
                printk(DM_GECKO_PREFIX "Unable to alloc io_for_block cache\n");
                goto out1;
        }

        if (!(io_job_cache = KMEM_CACHE(io_job, 0))) {
                printk(DM_GECKO_PREFIX "unable to alloc io_job cache\n");
                goto out2;
        }

        io_for_block_mempool = mempool_create_slab_pool(MIN_JOBS_IN_POOL,
                                                        io_for_block_cache);
        if (!io_for_block_mempool) {
                printk(DM_GECKO_PREFIX
		       "unable to alloc io_for_block mempool\n");
                goto out3;
        }

        io_job_mempool = mempool_create_slab_pool(MIN_JOBS_IN_POOL,
                                                  io_job_cache);
        if (!io_job_mempool) {
                printk(DM_GECKO_PREFIX "unable to alloc io_job mempool\n");
                goto out4;
        }

        /* The correctness of the algorithms rely on the assumption
         * that gecko_wqueue is a singlethreaded workqueue. */
        if (!(gecko_wqueue = create_singlethread_workqueue("geckod"))) {
                printk(DM_GECKO_PREFIX "unable to create geckod workqueue\n");
                goto out5;
        }
        INIT_WORK(&gecko_work, run_deferred_jobs);

        if (!(gecko_sync_metadata_wqueue =
              create_singlethread_workqueue("geckod-meta"))) {
                printk(DM_GECKO_PREFIX
                       "unable to create geckod-meta workqueue\n");
                goto out6;
        }


        if ((err = dm_register_target(&gecko_target)) < 0) {
                printk(DM_GECKO_PREFIX "register target failed %d\n", err);
                goto out7;
        }

        if (!(gecko_cleaning_wqueue =
              create_singlethread_workqueue("geckod-cleaning"))) {
                printk(DM_GECKO_PREFIX
                       "unable to create geckod-cleaning workqueue\n");
                goto out8;
        }

#if ZIGGY_DEBUG
        // ziggy: TEST
        printk(DM_GECKO_PREFIX "module loaded\n");
#endif
        return 0;
out8:  // ziggy: workqueue for cleaning
        destroy_workqueue(gecko_cleaning_wqueue);
out7:
        destroy_workqueue(gecko_sync_metadata_wqueue);
out6:
        destroy_workqueue(gecko_wqueue);
out5:
        mempool_destroy(io_job_mempool);
out4:
        mempool_destroy(io_for_block_mempool);
out3:
        kmem_cache_destroy(io_job_cache);
out2:
        kmem_cache_destroy(io_for_block_cache);
out1:
        return err;
}

static void __exit dm_gecko_exit(void)
{
        dm_unregister_target(&gecko_target);
        BUG_ON(!list_empty(&deferred_jobs));
        mempool_destroy(io_job_mempool);
        mempool_destroy(io_for_block_mempool);
        kmem_cache_destroy(io_job_cache);
        kmem_cache_destroy(io_for_block_cache);
        destroy_workqueue(gecko_cleaning_wqueue);
        destroy_workqueue(gecko_wqueue);
        destroy_workqueue(gecko_sync_metadata_wqueue);
        dmg_kcopyd_exit();
        printk(DM_GECKO_PREFIX "module unloaded\n");
}

module_init(dm_gecko_init);
module_exit(dm_gecko_exit);
MODULE_DESCRIPTION("Gecko: power saving log structured storage system");
MODULE_AUTHOR("Tudor Marian <tudorm@cs.cornell.edu>");
#ifndef MODULE_LICENSE
#define MODULE_LICENSE(a)
#endif
MODULE_LICENSE("Dual BSD/GPL");
