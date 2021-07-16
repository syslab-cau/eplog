#ifndef __CALCLOCK_H_
#define __CALCLOCK_H_

#include <linux/time.h>
#define BILLION 1000000000L

unsigned long long calclock(struct timespec64 *myclock,
                            unsigned long long *total_time,
                            unsigned long long *total_count)
{
    unsigned long long timedelay=0, temp=0, temp_n=0;
    if (myclock[1].tv_nsec >= myclock[0].tv_nsec) {
        temp = myclock[1].tv_sec - myclock[0].tv_sec;
        temp_n = myclock[1].tv_nsec - myclock[0].tv_nsec;
        timedelay = BILLION * temp + temp_n;
    } else {
        temp = myclock[1].tv_sec - myclock[0].tv_sec - 1;
        temp_n = BILLION + myclock[1].tv_nsec - myclock[0].tv_nsec;
        timedelay = BILLION * temp + temp_n;
    }

    __sync_fetch_and_add(total_time, timedelay);
    __sync_fetch_and_add(total_count, 1);

    return timedelay;
}

unsigned long long calclock2(struct timespec64 *myclock,
                             unsigned long long *total_time,
                             unsigned long long *total_count)
{
    unsigned long long timedelay=0, temp=0, temp_n=0;
    if (myclock[1].tv_nsec >= myclock[0].tv_nsec) {
        temp = myclock[1].tv_sec - myclock[0].tv_sec;
        temp_n = myclock[1].tv_nsec - myclock[0].tv_nsec;
        timedelay = BILLION * temp + temp_n;
    } else {
        temp = myclock[1].tv_sec - myclock[0].tv_sec - 1;
        temp_n = BILLION + myclock[1].tv_nsec - myclock[0].tv_nsec;
        timedelay = BILLION * temp + temp_n;
    }

    //tmp = __sync_fetch_and_add(total_time, timedelay);
    __sync_bool_compare_and_swap(total_time, *total_time, timedelay);
    __sync_fetch_and_add(total_count, 1);

    return timedelay;
}

#endif // __CALCLOCK_H_
