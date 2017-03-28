/*
  Replicate It: mutlichannel UDP proxy (proof of concept)

  Idea: send several packet copies via independent internet channels
        and use first arrived.

  Why you shouldn't use this code in production:
   - packet numbers aren't synchronized after one side restart/link loss
     (it means that connection stops working);
   - only static configuration, it is impossible to add/remove channel
     in runtime.

  How to biuld:
  gcc -Wall -O3 replicateit.c -o replicateit
  (maybe you need to add "-lrt" on some systems)

  How to use:
  let we have two internet connections on the server, addresses are SIP1 and
  SIP2; and two internet connections on the client, addresses CIP1 and CIP2.

  1. run on the server side
  replicateit -F SIP1:PORT -F SIP2:PORT -t SERVICE-IP:PORT
  2. run on the client side
  replicateit -f 127.0.0.1:PORT -T SIP1:PORT/CIP1 -T SIP2:PORT/CIP2
  3. tune client software to connict 127.0.0.1 instead of SERVICE-IP
  That's all.
  I tested it with OpenVPN - it works.
*/


#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/*
  Some constats here
*/

#define SINGATURE	"ReIt"

// Maximum packet payload size
#define MAXPACKSIZE	2048	// usualy MTU <= 1500, so it seems to be enough

// Maximum count of channels
#define MAXCHANNELS	4	// up to 3 replicas of data

// Size of the circular buffer (used to filter packet duplicates)
#define BITMAPLEN	512u	// one bit per packet

// It is stored as an array of integers, 32 or 64 bit each
#if ULONG_MAX == 0xffffffffffffffff
#  define BITMAPITEMLEN	64u	// we prefer to use native 64-bit arithmetic
#  define BITMAPITEMTYPE	uint64_t
#  define BIT		1ull
#else
#  define BITMAPITEMLEN	32u	// but we can use 32-bit as well
#  define BITMAPITEMTYPE	uint32_t
#  define BIT		1ul
#endif

#define BITMAPITEMS	(BITMAPLEN/BITMAPITEMLEN)

/*
  Data stuctures
*/

struct packet_header {
  char signature[4];
  uint32_t n;			// packet number
};

struct packet {
  struct packet_header h;
  uint8_t data[MAXPACKSIZE];
};

struct channel {
  int fd;
  struct sockaddr_in addr;      // address of another side
  uint8_t addr_is_known;        // 0 - unknown, 1 - dynamic, 2 - static
  int counter, counter_ok;	// for statistics only
};

/*
  Global variables
*/

struct channel ch[MAXCHANNELS]; // ch[0] - to communicate with proxied service
                                // all others to multichannel

int channels;                   // actual channels count

/*
  Main loop
*/

void serve(void)
{
  struct packet in0 = {.h = {.signature = SINGATURE} }; // for proxied packets
  struct packet in1;            // for replicas
  BITMAPITEMTYPE bm[BITMAPITEMS];     // bitmap for recieved packets
  int maxfd = 0;
  fd_set rfds;
  int i;
  int in0_n = 1, in1_n = 0;

  bzero(bm, sizeof(bm));

  for (;;) {
    FD_ZERO(&rfds);

    for (i = 0; i < channels; i++) {
      if (ch[i].fd > maxfd)
        maxfd = ch[i].fd;
      FD_SET(ch[i].fd, &rfds);
    }

    select(maxfd + 1, &rfds, NULL, NULL, NULL);

    if (FD_ISSET(ch[0].fd, &rfds)) {
      int l;
      struct sockaddr *addrP;
      socklen_t alen = sizeof(ch[0].addr);

      if (ch[0].addr_is_known < 2) {
        addrP = (struct sockaddr *) (&ch[0].addr);
      } else {
        addrP = NULL;
      }

      l = recvfrom(ch[0].fd, (void *) in0.data, sizeof(in0.data), 0, addrP,
                   &alen);
      if (l > 0) {
        if (ch[0].addr_is_known == 0)
          ch[0].addr_is_known = 1;

        in0.h.n = htonl(in0_n);
        in0_n++;
        for (i = 1; i < channels; i++)
          if (ch[i].addr_is_known)
            sendto(ch[i].fd, (void *) &in0, l + sizeof(in0.h), 0,
                   (struct sockaddr *) &(ch[i].addr), sizeof(ch[i].addr));
      }
    }
    for (i = 1; i < channels; i++) {
      if (FD_ISSET(ch[i].fd, &rfds)) {
        int l;
        struct sockaddr *addrP;
        socklen_t alen = sizeof(ch[i].addr);

        if (ch[i].addr_is_known < 2) {
          addrP = (struct sockaddr *) (&ch[i].addr);
        } else {
          addrP = NULL;
        }

        l = recvfrom(ch[i].fd, (void *) &in1, sizeof(in1), 0, addrP,
                     &alen);
        if (l > 0) {
          if (memcmp
              (in0.h.signature, in1.h.signature, sizeof(in0.h.signature))
              == 0) {
            int use;

            if (ch[i].addr_is_known == 0)
              ch[i].addr_is_known = 1;
            uint32_t in1_n_candidate = ntohl(in1.h.n);

            if (in1_n - in1_n_candidate > (uint32_t) INT32_MAX) {
              uint32_t a = in1_n_candidate / BITMAPITEMLEN, b =
                  in1_n / BITMAPITEMLEN;
              int i;

              for (i = 0; a != b && i < BITMAPITEMS; a--, i++)
                bm[a % BITMAPITEMS] = 0;
              use = 1;
              in1_n = in1_n_candidate;
            } else {
              uint32_t gap =
                  (in1_n & (~(BITMAPITEMLEN - 1))) -
                  (in1_n_candidate & (~(BITMAPITEMLEN - 1)));
              static uint32_t max_gap = 0;

              if (gap < BITMAPLEN) {
                if (bm[(in1_n_candidate / BITMAPITEMLEN) % BITMAPITEMS] &
                    (BIT << (in1_n_candidate % BITMAPITEMLEN))) {
                  use = 0;
                } else {
                  use = 1;
                }
              } else {
                if (gap > max_gap) {
                  printf
                      ("too old packet received! "
                       "BITMAPLEN current value is %u, gap is %u\n",
                       BITMAPLEN, gap);
                  max_gap = gap;
                }
                use = 0;
              }
            }

            if (use) {
              bm[(in1_n_candidate / BITMAPITEMLEN) % BITMAPITEMS] |=
                  BIT << (in1_n_candidate % BITMAPITEMLEN);
              ch[i].counter_ok++;
              if (ch[0].addr_is_known) {
                sendto(ch[0].fd, (void *) in1.data, l - sizeof(in1.h), 0,
                       (struct sockaddr *) &(ch[0].addr),
                       sizeof(ch[0].addr));
                ch[0].counter += l - sizeof(in1.h);
              }
            }

            ch[i].counter++;
            if (ch[i].counter >= 1000) {
              int j;
              static uint64_t last_time = 0;
              uint64_t time;
              struct timespec ts;

#ifdef _POSIX_MONOTONIC_CLOCK
              clock_gettime(CLOCK_MONOTONIC, &ts);
#else
              clock_gettime(CLOCK_REALTIME, &ts);
#endif

              time = ts.tv_sec * 1000000000ull + ts.tv_nsec;

              for (j = 1; j < channels; j++) {
                printf("%4d/%4d ", ch[j].counter_ok, ch[j].counter);
                ch[j].counter = 0;
                ch[j].counter_ok = 0;
              }
              if (last_time && last_time < time) {
                printf("%8.2fMbit/s",
                       ch[0].counter * 8 * 1000.0 / (time - last_time));
              }
              last_time = time;
              ch[0].counter = 0;
              printf("\n");
            }
          }
        }
      }
    }
  }
}

/*
  Ugly parser for strings like "127.0.0.1:5001"
*/

void split_string(char *text, char delimiter, char **part1, char **part2)
{
  int i = 0;

  *part2 = NULL;

  if (text)
    while (text[i]) {
      if (text[i] == delimiter) {
        text[i] = '\0';

        if (text[i + 1]) {
          *part2 = text + i + 1;
        }
        break;
      }
      i++;
    }
  if (i) {
    *part1 = text;
  } else {
    *part1 = NULL;
  }
}

/*
  Fill struct sockaddr_in using IPv4:PORT notation
  IPv6 isn't supported, what a shame
*/

void fill_addr(char *text, struct sockaddr_in *addrP)
{
  char *ip, *port;

  bzero(addrP, sizeof(*addrP));
  addrP->sin_family = AF_INET;

  split_string(text, ':', &ip, &port);

  if (ip == NULL || ip[0] == '*') {
    addrP->sin_addr.s_addr = htonl(INADDR_ANY);
  } else {
    inet_aton(ip, &(addrP->sin_addr));
  }
  addrP->sin_port = htons((port == NULL ? 0 : atoi(port)));
}

/*
  Create socket and init channel

  direction:
    1 - it is listening socket (we provide bind address only)
        text should be like [LOCAL_IP]:PORT
    2 - "connected" socket
        text should be like DST_IP:PORT[/SRC_IP[:PORT]]
*/

void init_ch(struct channel *chP, int direction, char *text)
{
  struct sockaddr_in addr;
  char *ba;

  bzero(chP, sizeof(struct channel));

  chP->fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
  if (direction == 1) {
    ba = text;
    chP->addr_is_known = 0;
  } else {
    char *sa;

    split_string(text, '/', &sa, &ba);
    fill_addr(sa, &(chP->addr));
    chP->addr_is_known = 2;
  }
  fill_addr(ba, &addr);
  bind(chP->fd, (struct sockaddr *) &addr, sizeof(addr));
}

/*
  Parse command line arguments here
*/

int main(int argc, char **argv)
{
  int mode = 0;
  int ch0_init_done = 0;
  char c;

  channels = 1;
  ch0_init_done = 0;
  while ((c = getopt(argc, argv, "f:F:t:T:")) != -1)
    switch (c) {
    case 'f':
      if (mode == 2)
        exit(1);
      mode = 1;
      if (ch0_init_done)
        exit(1);
      ch0_init_done = 1;
      init_ch(&(ch[0]), 1, optarg);
      break;
    case 'T':
      if (mode == 2)
        exit(1);
      mode = 1;
      if (channels>=MAXCHANNELS-1)
        exit(1);
      init_ch(&(ch[channels]), 2, optarg);
      channels++;
      break;
    case 't':
      if (mode == 1)
        exit(1);
      mode = 2;
      if (ch0_init_done)
        exit(1);
      ch0_init_done = 1;
      init_ch(&(ch[0]), 2, optarg);
      break;
    case 'F':
      if (mode == 1)
        exit(1);
      mode = 2;
      if (channels>=MAXCHANNELS-1)
        exit(1);
      init_ch(&(ch[channels]), 1, optarg);
      channels++;
      break;
    default:
      exit(1);
    }

  if (ch0_init_done == 0) {
  }

  if (channels < 2) {
    exit(1);
  }

  serve();

  return 0;
}
