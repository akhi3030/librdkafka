
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>


#include "rdkafka.h"


static inline void __ss_try_fail(const char* arg, int rc,
                                 const char* file, int line)
{
        fprintf(stderr, "ERROR: TRY(%s) failed\n", arg);
        fprintf(stderr, "ERROR: at %s:%d\n", file, line);
        fprintf(stderr, "ERROR: rc=%d errno=%d (%s)\n",
                rc, errno, strerror(errno));
#ifndef NDEBUG
        abort();
#else
        exit(1);
#endif
}


/**
 * \brief Helper macro for testing success
 */
#define TRY(x)                                                          \
        do {                                                            \
                int __rc = (x);                                         \
                if( __rc < 0 ) {                                        \
                        __ss_try_fail(#x, __rc, __FILE__, __LINE__);    \
                        exit(1);                                        \
                }                                                       \
        } while( 0 )



static inline void __ss_test_fail(const char* arg, const char* file, int line)
{
        fprintf(stderr, "ERROR: TEST(%s) failed\n", arg);
        fprintf(stderr, "ERROR: at %s:%d\n", file, line);
        fprintf(stderr, "ERROR: errno=%d (%s)\n", errno, strerror(errno));
#ifndef NDEBUG
        abort();
#else
        exit(1);
#endif
}


/**
 * \brief Helper macro for testing success
 */
#define TEST(x)                                                 \
        do {                                                    \
                if( ! (x) ) {                                   \
                        __ss_test_fail(#x, __FILE__, __LINE__); \
                        exit(1);                                \
                }                                               \
        } while( 0 )


#define N_PKTS 10000

struct stats {
        uint64_t finished;
        uint64_t eagain;
} stats;


rd_kafka_t *rk;
rd_kafka_topic_t *rkt;
#define BUFLEN 512
char buf[BUFLEN] = "hello";


static double get_avg_diff(int64_t* diffs)
{
        int64_t tot = 0;
        for( int i = N_PKTS / 10; i < N_PKTS; ++i )
                tot += diffs[i];
        return ((double) tot) / N_PKTS;
}


static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage,
                      void *opaque)
{
        if (rkmessage->err) {
                fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
                TEST( 0 );
        } else {
                ++stats.finished;
        }
}


int produce_one(void)
{
        int rc = rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA,
                                  RD_KAFKA_MSG_F_COPY, buf, BUFLEN,
                                  NULL, 0, NULL);
        if( rc == 0 ) {
                return 0;
        }
        TEST( errno == ENOBUFS );
        ++stats.eagain;
        return -1;
}


static inline int64_t timespec_diff_ns(struct timespec a, struct timespec b)
{
  return (a.tv_sec - b.tv_sec) * (int64_t) 1000000000
    + (a.tv_nsec - b.tv_nsec);
}


double run(int sleep_time)
{
        int64_t diffs[N_PKTS];

        int i = 0;
        while( i < N_PKTS ) {
                if( sleep_time != 0 )
                        usleep(sleep_time);

                struct timespec start, end;
                TRY( clock_gettime(CLOCK_REALTIME, &start) );
                int rc = produce_one();
                TRY( clock_gettime(CLOCK_REALTIME, &end) );
                if( rc != 0 ) {
                        rd_kafka_poll(rk, 0);
                } else {
                        diffs[i] = timespec_diff_ns(end, start) / 1000;
                        ++i;
                }
        }
        while( stats.finished < N_PKTS )
                rd_kafka_poll(rk, 0);

        return get_avg_diff(diffs);
}


void print_stats(double diff)
{
        printf("diff=%lfus eagain=%ld\n", diff, stats.eagain);
        memset(&stats, 0, sizeof(stats));
}


int main(int argc, char **argv)
{
        if( argc != 3 ) {
                printf("Usage: %s <usleep-time> <linger-time>\n", argv[0]);
                exit(1);
        }
        int sleep_time = atoi(argv[1]);
        const char* linger = argv[2];
        rd_kafka_conf_t *conf;
        char errstr[512];
        const char *brokers = "localhost";
        const char *topic = "foo";

        conf = rd_kafka_conf_new();
        TEST( rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                                errstr, sizeof(errstr)) == RD_KAFKA_CONF_OK );

        TEST( rd_kafka_conf_set(conf, "linger.ms", linger,
                                errstr, sizeof(errstr)) == RD_KAFKA_CONF_OK );

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        TEST( rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)) );
        TEST( rkt = rd_kafka_topic_new(rk, topic, NULL) );

        for( int i = 0; i < 10; ++i ) {
                double diff = run(sleep_time);
                print_stats(diff);
        }
        return 0;
}
