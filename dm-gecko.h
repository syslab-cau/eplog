#ifndef __DM_GECKO_H_
#define __DM_GECKO_H_

#include "dmg-kcopyd.h"

struct io_job {
        struct list_head list;
        struct dm_gecko *dmg;
        int rw;                        /* READ or WRITE */
        struct io_for_block *parent;   /* if NULL, the job is deferred */
        void *page;                    /* for read-modify-update cycles */
        struct bio *bio;               /* if NULL this is a gc IO */
        blkcnt_t v_block;              /* virtual block */
        blkcnt_t l_block;              /* linear block */
        /* for whole segment gc */
        struct dm_dev_seg *dest_dev;  /*zgy: only for gc kcopyd callback*/
        unsigned int seg_idx;  /*zgy: only for gc kcopyd callback*/
        struct valid_blks_info *blks_info;  /* array */
        unsigned int nr_valid_blks;
        struct kcopyd_job *copy_job;  // zgy: bindind kcopy context to get the page list
        int reclaim_zone_idx;
};

#endif // __DM-GECKO_H_
