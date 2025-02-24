#include "pgm.h"

#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <mqueue.h>

#include <sys/socket.h>
#include <netdb.h>

#include <sys/syscall.h>

#include <set>
#include <queue>
#include <string>
#include <sstream>

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/filesystem.hpp>
#include <boost/functional/hash.hpp>

#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/bellman_ford_shortest_paths.hpp>
#include <boost/property_map/property_map.hpp>

#if defined(PGM_USE_PTHREAD_SYNC)
typedef pthread_mutex_t pgm_lock_t;
typedef pthread_cond_t  pgm_cv_t;
#elif defined(PGM_USE_PGM_SYNC)
#include "spinlock.h"
#include "condvar.h"
typedef spinlock_t      pgm_lock_t;
typedef cv_t            pgm_cv_t;

#if PGM_SPINLOCK_TYPE == 1
__thread queuenode_t thread_qnode;
#endif
#endif

#include "ring.h"

using namespace std;
using namespace boost;
using namespace boost::interprocess;
using namespace boost::filesystem;


// TODO LIST:
//  * Ring buffers in shared memory.

#ifndef PGM_CONFIG
#error "pgm/include/config.h not included!"
#endif

#if (PGM_MAX_IN_DEGREE > 32 && PGM_MAX_IN_DEGREE <= 64)
typedef uint64_t pgm_fd_mask_t;
#elif (PGM_MAX_IN_DEGREE > 0 && PGM_MAX_IN_DEGREE <= 32)
typedef uint32_t pgm_fd_mask_t;
#else
typedef uint32_t pgm_fd_mask_t;
#endif

#define UNCLAIMED_NODE -1

static __thread char errnostr_buf[80];

static inline pid_t pgm_gettid(void)
{
	pid_t p = syscall(__NR_gettid);
	return p;
}

// Used to report system FAILUREs.
#define F(fmt, ...) \
do { \
	fprintf(stderr, "FAILURE: %s:%d (%d:%s): " fmt, \
		__FUNCTION__, __LINE__, \
		errno, strerror_r(errno, errnostr_buf, sizeof(errnostr_buf)), \
		## __VA_ARGS__); \
} while(0)

// Used to report user ERRORS / API misuse
#define E(fmt, ...) \
do { \
	fprintf(stderr, "ERROR: " fmt, ## __VA_ARGS__); \
} while(0)

// Used to WARN the user of potential badness
#define W(fmt, ...) \
do { \
	fprintf(stderr, "WARNING: " fmt, ## __VA_ARGS__); \
} while(0)

///////////////////////////////////////////////////
//            Process-Level Globals              //
///////////////////////////////////////////////////

static string gMemName;
static __thread bool gIsGraphMaster = false;
static managed_shared_memory *gGraphSharedMem = 0;
static struct pgm_graph* gGraphs = 0;
static path gGraphPath;

///////////////////////////////////////////////////
//         Internal PGM Data Structures          //
///////////////////////////////////////////////////

typedef int (*edge_op_t)(struct pgm_graph* g,
				struct pgm_node* p, struct pgm_node* c,
				struct pgm_edge* e);
typedef edge_op_t init_t;
typedef edge_op_t open_t;
typedef edge_op_t destroy_t;

typedef int (*close_t)(struct pgm_edge* e);
typedef ssize_t (*read_t)(struct pgm_edge* e, void* buf, size_t nbytes);
typedef ssize_t (*write_t)(struct pgm_edge* e, const void* buf, size_t nbytes);

struct pgm_edge_ops
{
	init_t init;
	open_t open_consumer;
	open_t open_producer;
	close_t close_consumer;
	close_t close_producer;
	destroy_t destroy;
	read_t read;
	write_t write;
};

struct pgm_edge
{
	char name[PGM_EDGE_NAME_LEN];

	// id of producer and consumer
	int producer;
	int consumer;

	// flag set if edge is a back-edge
	bool is_backedge;

	// edge type and operations
	edge_attr_t	attr;
	struct pgm_edge_ops const* ops;

	// number of skipped edges
	size_t nr_skips;

	// number of accumulated tokens
	// (used by signaled edges)
	size_t nr_pending;

	// the remaining fields are used by data-passing edges

	union
	{
		// fields for standard data-passing IPCs
		struct
		{
			// fd_out and fd_in may be the same
			// if different ends of FIFOs are
			// opened by different processes.
			int fd_out;
			int fd_in;

			// counter for determining location of the message
			// header contained within received data.
			size_t next_tag;
		};
		// fields for ring buffer IPC
		struct
		{
			struct ring ringbuf;
			volatile pgm_command_t ring_cmd;
		};
	};

	// buffer for sending data
	struct pgm_memory_hdr* buf_out;

	// buffer for receiving data
	struct pgm_memory_hdr* buf_in;
};

static inline bool is_signal_driven(const struct pgm_edge_attr* attr)
{
	return (attr->type & __PGM_SIGNALED);
}

static inline bool is_data_passing(const struct pgm_edge_attr* attr)
{
	return (attr->type & __PGM_DATA_PASSING);
}

static inline bool is_signal_driven(const struct pgm_edge* e)
{
	return is_signal_driven(&e->attr);
}

static inline bool is_data_passing(const struct pgm_edge* e)
{
	return is_data_passing(&e->attr);
}


struct pgm_node
{
	char name[PGM_NODE_NAME_LEN];

	// in/out hold indices to edges
	int in[PGM_MAX_IN_DEGREE];
	int out[PGM_MAX_OUT_DEGREE];

	int nr_in;
	int nr_out;

	// number of edges that pass data
	int nr_in_data;
	// number of edges signaled using condition variables
	int nr_in_signaled;

	// number of data-passing edges that are backedges
	int nr_in_data_backedges;
	// number of signal-based edges that are backedges
	int nr_in_signaled_backedges;

	// bit is set if edge is a singaling edge.
	pgm_fd_mask_t signal_edge_mask;

	// number of termination signals received
	int nr_terminate_signals;
	int nr_terminate_msgs;

	// Linux thread ID of thread claiming ownership
	pid_t owner;

	// Pointer to user-attached user data
	void* userdata;

	// only used if inbound edges are signal-based
	pgm_lock_t	lock;
	pgm_cv_t	wait;

};

struct pgm_graph
{
	int in_use;
	char name[PGM_GRAPH_NAME_LEN];

	pthread_mutex_t lock;

	int nr_nodes;
	int nr_edges;

	struct pgm_node nodes[PGM_MAX_NODES];
	struct pgm_edge edges[PGM_MAX_EDGES];
};

/* default to simple signal-based, non-data-passing IPC */
static const edge_attr_t default_edge = {
	.nr_produce   = 1,
	.nr_consume   = 1,
	.nr_threshold = 1,
	.type         = pgm_cv_edge,
};

static inline int is_valid_handle(graph_t graph)
{
	return(graph >= 0 && graph <= PGM_MAX_GRAPHS);
}

static inline int is_valid_graph(graph_t graph)
{
	return (gGraphs != 0) && is_valid_handle(graph) && gGraphs[graph].in_use;
}


///////////////////////////////////////////////////
//     IPC Functions for Data-Passing Edges      //
///////////////////////////////////////////////////

// forward decl. needed for allocating edge buffers
struct pgm_memory_hdr* __pgm_malloc_edge_buf(struct pgm_graph* g,
				struct pgm_edge* e, bool is_producer);

/************* DUMMY IPC ROUTINES ****************/

static int dummy_edge_op(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	return 0;
}

static int dummy_edge_close(struct pgm_edge* e)
{
	return 0;
}

static ssize_t dummy_edge_read(struct pgm_edge* e, void* buf, size_t nbytes)
{
	return 0;
}

static ssize_t dummy_edge_write(struct pgm_edge* e, const void* buf, size_t nbytes)
{
	return 0;
}

/************* RING IPC ROUTINES *****************/
static int ring_init(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;

	/* nr_produce/nr_consume interpreted as bytes passed between
	   producer and consumer, per invocation. Impl. requires that
	   nr_produce == nr_consume. */
	if (edge->attr.nr_produce != edge->attr.nr_consume)
		goto out;

	ret = init_ring(&edge->ringbuf, edge->attr.nmemb, edge->attr.nr_produce);

out:
	return ret;
}

static int ring_open_consumer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	edge->buf_in = __pgm_malloc_edge_buf(g, edge, false);
	return 0;
}

static int ring_open_producer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	edge->buf_out = __pgm_malloc_edge_buf(g, edge, true);
	return 0;
}

static int ring_close_consumer(pgm_edge* edge)
{
	free(edge->buf_in);
	edge->buf_in = 0;
	return 0;
}

static int ring_close_producer(pgm_edge* edge)
{
	free(edge->buf_out);
	edge->buf_out = 0;
	return 0;
}

static int ring_destroy(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	free_ring(&edge->ringbuf);
	return 0;
}

static ssize_t ring_read(struct pgm_edge* e, void* buf, size_t nbytes)
{
	assert(nbytes == e->attr.nr_produce);
	assert(nbytes == e->attr.nr_consume);

	switch(nbytes)
	{
	case 8:
		read_ring(&e->ringbuf, (uint64_t*)buf);
		break;
	case 4:
		read_ring(&e->ringbuf, (uint32_t*)buf);
		break;
	case 2:
		read_ring(&e->ringbuf, (uint16_t*)buf);
		break;
	case 1:
		read_ring(&e->ringbuf, (uint8_t*)buf);
		break;
	default:
		read_vec_ring(&e->ringbuf, buf, nbytes);
	}
	return nbytes; /* assume always successful */
}

static ssize_t ring_write(struct pgm_edge* e, const void* buf, size_t nbytes)
{
	assert(nbytes == e->attr.nr_produce);
	assert(nbytes == e->attr.nr_consume);

	switch(nbytes)
	{
	case 8:
		write_ring(&e->ringbuf, *(uint64_t*)buf);
		break;
	case 4:
		write_ring(&e->ringbuf, *(uint32_t*)buf);
		break;
	case 2:
		write_ring(&e->ringbuf, *(uint16_t*)buf);
		break;
	case 1:
		write_ring(&e->ringbuf, *(uint8_t*)buf);
		break;
	default:
		write_vec_ring(&e->ringbuf, buf, nbytes);
	}
	return nbytes; /* assume always successful */
}

static const struct pgm_edge_ops pgm_ring_edge_ops =
{
	.init = ring_init,
	.open_consumer = ring_open_consumer,
	.open_producer = ring_open_producer,
	.close_consumer = ring_close_consumer,
	.close_producer = ring_close_producer,
	.destroy = ring_destroy,
	.read = ring_read,
	.write = ring_write,
};


/************* FIFO IPC ROUTINES *****************/
static std::string fifo_name(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	boost::hash<std::string> string_hash;
	size_t hash = string_hash(gGraphPath.string());
	stringstream ss;
	ss<<hash<<"_"<<g->name<<"_"<<producer->name<<"_"<<consumer->name
			<<"_"<<edge->name<<".edge";
	return ss.str();
}

static int fifo_create(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;

	if(gGraphPath.string().empty())
	{
		F("Graph directory not set. Did you call pgm_init_process_local() "
		  "insted of pgm_init()?\n");
	}
	else
	{
		string fifoName(fifo_name(g, producer, consumer, edge));

		path fifoPath(gGraphPath);
		fifoPath /= fifoName;

		// Remove any old FIFO that may exist.
		remove(fifoPath);

		// TODO: See what boost can do here.
		ret = mkfifo(fifoPath.string().c_str(), S_IRUSR | S_IWUSR);
		if(0 != ret)
		{
			F("Failed to make FIFO %s\n", fifoPath.string().c_str());
		}
	}
	return ret;
}

static int fifo_open_consumer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;
	if(gGraphPath.string().empty())
	{
		F("Graph directory not set. Did you call pgm_init_process_local() "
		  "insted of pgm_init()?\n");
	}
	else
	{
		path fifoPath(gGraphPath);
		fifoPath /= fifo_name(g, producer, consumer, edge);
		edge->fd_in = open(fifoPath.string().c_str(), O_RDONLY | O_NONBLOCK);

		if(edge->fd_in != -1)
		{
			edge->buf_in = __pgm_malloc_edge_buf(g, edge, false);
			ret = 0;
		}
		else
		{
			F("Could not open inbound edge %s/%s (FIFO)\n", g->name, edge->name);
		}
	}
	return ret;
}

static int fifo_open_producer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;

	if(gGraphPath.string().empty())
	{
		F("Graph directory not set. Did you call pgm_init_process_local() "
		  "insted of pgm_init()?\n");
	}
	else
	{
		path fifoPath(gGraphPath);
		fifoPath /= fifo_name(g, producer, consumer, edge);

		const int timeout = 60;
		const int start_time = time(0);
		__sync_synchronize();
		do
		{
			edge->fd_out = open(fifoPath.string().c_str(),
				O_WRONLY | O_NONBLOCK);
			if(edge->fd_out == -1)
			{
				if(errno != ENXIO)
				{
					F("Could not open outbound edge %s/%s (FIFO)\n",
					  g->name, edge->name);
					break;
				}
				else
				{
					if(time(0) - start_time > timeout)
					{
						F("Could not open outbound edge %s/%s (FIFO)\n",
						   g->name, edge->name);
						break;
					}
					usleep(1000); // wait for a millisecond
				}
			}
			else
			{
				ret = 0;
			}
		}while(ret == -1);

		if(!ret)
		{
			edge->buf_out = __pgm_malloc_edge_buf(g, edge, true);
		}
	}
	return ret;
}

static int fifo_close_consumer(pgm_edge* edge)
{
	int ret = close(edge->fd_in);
	if(!ret)
	{
		edge->fd_in = 0;
		free(edge->buf_in);
		edge->buf_in = 0;
	}
	return ret;
}

static int fifo_close_producer(pgm_edge* edge)
{
	int ret = close(edge->fd_out);
	if(!ret)
	{
		edge->fd_out = 0;
		free(edge->buf_out);
		edge->buf_out = 0;
	}
	return ret;
}

static int fifo_destroy(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;

	if(gGraphPath.string().empty())
	{
		F("Graph directory not set. Did you call pgm_init_process_local() "
		  "insted of pgm_init()?\n");
	}
	else
	{
		string fifoName(fifo_name(g, producer, consumer, edge));
		path fifoPath(gGraphPath);
		fifoPath /= fifoName;

		if(edge->buf_in != 0 || edge->buf_out != 0)
		{
			W("Edge has not been closed: (producer:%s, consumer:%s)!\n",
				(edge->buf_out != 0) ? "open" : "closed",
				(edge->buf_in != 0) ? "open" : "closed");
		}

		if(exists(fifoPath) &&
		   remove(fifoPath))
		{
			ret = 0;
		}
	}

	return ret;
}

static ssize_t fifo_read(struct pgm_edge* e, void* buf, size_t nbytes)
{
	return read(e->fd_in, buf, nbytes);
}

static ssize_t fifo_write(struct pgm_edge* e, const void* buf, size_t nbytes)
{
	return write(e->fd_out, buf, nbytes);
}

static const struct pgm_edge_ops pgm_fifo_edge_ops =
{
	.init = fifo_create,
	.open_consumer = fifo_open_consumer,
	.open_producer = fifo_open_producer,
	.close_consumer = fifo_close_consumer,
	.close_producer = fifo_close_producer,
	.destroy = fifo_destroy,
	.read = fifo_read,
	.write = fifo_write,
};


/************* MQ IPC ROUTINES *****************/

inline std::string mq_name(pgm_graph* g,
				pgm_node* p, pgm_node* c,
				pgm_edge* e)
{
	// use same naming scheme as FIFOs
	return fifo_name(g, p, c, e);
}

static int mq_create(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;
	path mqPath("/"); // msg queues always start at root
	mqPath /= mq_name(g, producer, consumer, edge);

	// MQs are created on open, so we just remove any
	// pre-existing MQ here.
	ret = mq_unlink(mqPath.string().c_str());
	if(ret == -1 && errno == ENOENT)
		ret = 0; // it's okay if the MQ didn't exist

	return ret;
}

static int mq_open_consumer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	path mqPath("/"); // msg queues always start at root
	mqPath /= mq_name(g, producer, consumer, edge);

	mq_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.mq_flags = O_NONBLOCK;
	attr.mq_maxmsg = edge->attr.mq_maxmsg;
	attr.mq_msgsize = edge->attr.nr_produce + sizeof(pgm_command_t);

	::mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
	edge->fd_in = mq_open(mqPath.string().c_str(), O_CREAT | O_NONBLOCK | O_RDONLY, mode, &attr);
	if(edge->fd_in == -1)
	{
		F("Could not open inbound edge %s/%s (MQ)\n", g->name, edge->name);
		return -1;
	}

	edge->buf_in = __pgm_malloc_edge_buf(g, edge, false);

	return 0;
}

static int mq_open_producer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	path mqPath("/"); // msg queues always start at root
	mqPath /= mq_name(g, producer, consumer, edge);

	mq_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.mq_flags = O_NONBLOCK;
	attr.mq_maxmsg = edge->attr.mq_maxmsg;
	attr.mq_msgsize = edge->attr.nr_produce + sizeof(pgm_command_t);

	::mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
	edge->fd_out = mq_open(mqPath.string().c_str(), O_CREAT | O_NONBLOCK | O_WRONLY, mode, &attr);
	if(edge->fd_out == -1)
	{
		F("Could not open outbound edge %s/%s (MQ)\n", g->name, edge->name);
		return -1;
	}

	edge->buf_out = __pgm_malloc_edge_buf(g, edge, true);

	return 0;
}

static int mq_close_consumer(pgm_edge* edge)
{
	int ret = mq_close(edge->fd_in);
	if(!ret)
	{
		edge->fd_in = 0;
		free(edge->buf_in);
		edge->buf_in = 0;
	}

	return ret;
}

static int mq_close_producer(pgm_edge* edge)
{
	int ret = mq_close(edge->fd_out);
	if(!ret)
	{
		edge->fd_out = 0;
		free(edge->buf_out);
		edge->buf_out = 0;
	}
	return ret;
}

static int mq_destroy(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	path mqPath("/"); // msg queues always start at root
	mqPath /= mq_name(g, producer, consumer, edge);

	if(edge->buf_in != 0 || edge->buf_out != 0)
	{
		// let the memory leak
		W("Edge has not been closed: (producer:%s, consumer:%s)!\n",
			(edge->buf_out != 0) ? "open" : "closed",
			(edge->buf_in != 0) ? "open" : "closed");
	}

	return mq_unlink(mqPath.string().c_str());
}

static ssize_t mq_read(struct pgm_edge* e, void* buf, size_t nbytes)
{
	ssize_t ret = mq_receive(e->fd_in, (char*)buf, nbytes, 0);
	return ret;
}

static ssize_t mq_write(struct pgm_edge* e, const void* buf, size_t nbytes)
{
	ssize_t ret = mq_send(e->fd_out, (const char*)buf, nbytes, 1);
	// all bytes are sent upon success
	if(ret == 0)
		ret = nbytes;
	return ret;
}

static const struct pgm_edge_ops pgm_mq_edge_ops =
{
	.init = mq_create,
	.open_consumer = mq_open_consumer,
	.open_producer = mq_open_producer,
	.close_consumer = mq_close_consumer,
	.close_producer = mq_close_producer,
	.destroy = mq_destroy,
	.read = mq_read,
	.write = mq_write,
};


/************* TCP IPC ROUTINES *****************/

static int sock_stream_create(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	return 0;
}

static int sock_stream_open_consumer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;
	int s;
	int sfd = -1;
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	char portnum[32] = {0};

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = 0;
	hints.ai_protocol = 0;

	snprintf(portnum, sizeof(portnum), "%d", edge->attr.port);

	s = getaddrinfo(edge->attr.node, portnum, &hints, &result);
	if(s)
	{
		F("getaddrinfo() failed. err: %d\n", s);
		goto out;
	}

	for(rp = result; rp != 0; rp = rp->ai_next)
	{
		sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if(sfd == -1)
			continue;
		if(connect(sfd, rp->ai_addr, rp->ai_addrlen) != -1)
			break;
		close(sfd);
	}
	if(rp == 0)
	{
		F("socket() or connect() failed.\n");
		goto out;
	}

	freeaddrinfo(result);

	edge->fd_in = sfd;
	edge->buf_in = __pgm_malloc_edge_buf(g, edge, false);
	ret = 0;

out:
	return ret;
}

static int sock_stream_open_producer(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	int ret = -1;
	int s;
	int sfd = -1;
	char portnum[32] = {0};

	struct addrinfo hints;
	struct addrinfo *result, *rp;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;
	hints.ai_protocol = 0;

	snprintf(portnum, sizeof(portnum), "%d", edge->attr.port);

	s = getaddrinfo(0, portnum, &hints, &result);
	if(s)
	{
		F("getaddrinfo() failed. err:%d\n", s);
		goto out;
	}

	for(rp = result; rp != 0; rp = rp->ai_next)
	{
		sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sfd == -1)
			continue;
		if (bind(sfd, rp->ai_addr, rp->ai_addrlen) == 0)
			break;
		close(sfd);
	}
	if(rp == 0)
	{
		F("socket() or bind() failed.\n");
		goto out;
	}

	freeaddrinfo(result);

	ret = listen(sfd, 1);
	if(ret < 0)
	{
		F("listen() failed\n");
		goto out;
	}

	ret = accept(sfd, 0, 0);
	if(ret < 0)
	{
		F("accept() failed\n");
		goto out;
	}

	edge->fd_out = ret;
	edge->attr.fd_prod_socket = sfd;
	edge->buf_out = __pgm_malloc_edge_buf(g, edge, true);
	ret = 0;
out:
	return ret;
}

static int sock_stream_close_consumer(pgm_edge* edge)
{
	int ret;
	ret = close(edge->fd_in);
	if(!ret)
	{
		edge->fd_in = 0;
		free(edge->buf_in);
		edge->buf_in = 0;
	}
	return ret;
}

static int sock_stream_close_producer(pgm_edge* edge)
{
	int ret;
	ret = close(edge->fd_out);
	ret |= close(edge->attr.fd_prod_socket);
	if(!ret)
	{
		edge->fd_out = 0;
		edge->attr.fd_prod_socket = 0;
		free(edge->buf_out);
		edge->buf_out = 0;
	}
	return ret;
}

static int sock_stream_destroy(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	return 0;
}

static ssize_t sock_stream_read(struct pgm_edge* e, void* buf, size_t nbytes)
{
	int flags = MSG_DONTWAIT;
	return recv(e->fd_in, buf, nbytes, flags);
}

static ssize_t sock_stream_write(struct pgm_edge* e, const void* buf, size_t nbytes)
{
	int flags = MSG_NOSIGNAL | MSG_DONTWAIT;
	return send(e->fd_out, buf, nbytes, flags);
}

static const struct pgm_edge_ops pgm_sock_stream_edge_ops =
{
	.init = sock_stream_create,
	.open_consumer = sock_stream_open_consumer,
	.open_producer = sock_stream_open_producer,
	.close_consumer = sock_stream_close_consumer,
	.close_producer = sock_stream_close_producer,
	.destroy = sock_stream_destroy,
	.read = sock_stream_read,
	.write = sock_stream_write,
};


/************* CV IPC ROUTINES *****************/

static int cv_create(pgm_graph* g,
				pgm_node* producer, pgm_node* consumer,
				pgm_edge* edge)
{
	edge->nr_pending = edge->attr.nr_init;
	return 0;
}

static const struct pgm_edge_ops pgm_cv_edge_ops =
{
	.init = cv_create,
	.open_consumer = dummy_edge_op,
	.open_producer = dummy_edge_op,
	.close_consumer = dummy_edge_close,
	.close_producer = dummy_edge_close,
	.destroy = dummy_edge_op,
	.read = dummy_edge_read,
	.write = dummy_edge_write,
};


///////////////////////////////////////////////////
//         Memory Management Routines            //
///////////////////////////////////////////////////

#define PGM_COOKIE 0x00100100
#define PGM_USERMEM_ALIGNMENT  16
#define PGM_USERMEM_PADDING   (size_t(PGM_USERMEM_ALIGNMENT - 1))
#define PGM_USERMEM_EXTRA     (sizeof(pgm_memory_hdr_t) + sizeof(pgm_offset_t) + PGM_USERMEM_PADDING)
#define PGM_TRANSMISSION_TAGS (sizeof(pgm_command_t))

//////////////////////////////////////////////////////////////////////
// Memory Layout:                                                   //
//                                                                  //
//  +------------------------------------------------------------+  //
//  | hdr | pad. | off. | trans. tag | 16-byte aligned user data |  //
//  +------------------------------------------------------------+  //
//                                                                  //
//  hdr: Allocation header information.                             //
//  pad: 0 to 15 bytes of padding. Variable.                        //
//  off: Offset from userpointer to hdr (i.e., hdr == user - off)   //
//  trans. tags: Space used by data-passing edges to pass extra     //
//               informataion (e.g., termination)                   //
//  data: User data (16-byte aligned).                              //
//                                                                  //
//////////////////////////////////////////////////////////////////////

static const edge_t BAD_EDGE =
{
	.graph = -1,
	.edge = -1,
};

typedef struct pgm_memory_hdr
{
	size_t usersize;
	edge_t assigned_edge;
	unsigned int cookie;
	char producer_flag:1; // valid iff assigned_edge != BAD_EDGE
} pgm_memory_hdr_t;

typedef pgm_command_t pgm_offset_t;

static inline pgm_memory_hdr_t* pgm_get_mem_header(void* userptr)
{
	pgm_offset_t off = ((pgm_offset_t*)userptr)[-2];
	if(off > PGM_USERMEM_EXTRA)
		return 0; // offset is too big to be valid
	pgm_memory_hdr_t* ptr = (pgm_memory_hdr_t*)((char*)userptr - off);
	return ptr;
}

static inline pgm_memory_hdr_t* pgm_get_mem_header_safe(void* userptr)
{
	pgm_memory_hdr_t* ptr = pgm_get_mem_header(userptr);
	if(!ptr || ptr->cookie != PGM_COOKIE)
		return 0;
	return ptr;
}

static inline void* pgm_get_user_ptr(pgm_memory_hdr_t* mem)
{
	return (void*)(((size_t)mem + PGM_USERMEM_EXTRA) & ~PGM_USERMEM_PADDING);
}

static inline int is_producer_buf(void* userptr)
{
	pgm_memory_hdr_t* ptr = pgm_get_mem_header_safe(userptr);
	if(!ptr)
		return 0;
	if(ptr->assigned_edge.graph == BAD_EDGE.graph &&
	   ptr->assigned_edge.edge == BAD_EDGE.edge)
		return 0;
	return (ptr->producer_flag == 1);
}

static inline int is_consumer_buf(void* userptr)
{
	pgm_memory_hdr_t* ptr = pgm_get_mem_header_safe(userptr);
	if(!ptr)
		return 0;
	if(ptr->assigned_edge.graph == BAD_EDGE.graph &&
	   ptr->assigned_edge.edge == BAD_EDGE.edge)
		return 0;
	return (ptr->producer_flag == 0);
}

static inline int is_buf_assigned(void* userptr, edge_t* e = NULL)
{
	pgm_memory_hdr_t* ptr = pgm_get_mem_header_safe(userptr);
	if(!ptr)
		return 0;
	if(ptr->assigned_edge.graph == BAD_EDGE.graph &&
	   ptr->assigned_edge.edge == BAD_EDGE.edge)
		return 0;
	if(e)
		*e = ptr->assigned_edge;
	return 1;
}

static void* pgm_malloc(size_t nbytes)
{
	char* buf;

	// ensure the header is not to big to be tracked by offset
	assert(PGM_USERMEM_EXTRA <= (size_t)~((pgm_offset_t)0));

	buf = (char*)malloc(nbytes + PGM_TRANSMISSION_TAGS + PGM_USERMEM_EXTRA);

	if(!buf)
		return 0;

	// record header information
	pgm_memory_hdr_t* hdr = (pgm_memory_hdr_t*)buf;
	hdr->usersize = nbytes;
	hdr->assigned_edge = BAD_EDGE;
	hdr->cookie = PGM_COOKIE;

	char* ptr = (char*)pgm_get_user_ptr(hdr);

	// record an offset to the head of the allocated memory
	((pgm_offset_t*)ptr)[-2] = ptr - buf;

	return ptr;
}

void pgm_free(void* userptr)
{
	pgm_memory_hdr_t* hdr;

	if(!userptr)
		return;

	hdr = pgm_get_mem_header_safe(userptr);

	if(!hdr)
	{
		E("Bad pointer!\n");
		return;
	}

	if(is_buf_assigned(userptr))
	{
		W("Buffer %p may still be in use by an edge!\n", userptr);
	}

	free(hdr);
}

pgm_memory_hdr_t* __pgm_malloc_edge_buf(struct pgm_graph* g, struct pgm_edge* e, bool is_producer)
{
	pgm_memory_hdr_t* mem = 0;
	size_t usernbytes = (is_producer) ? e->attr.nr_produce : e->attr.nr_consume;
	void* uptr = pgm_malloc(usernbytes);

	if(!uptr)
		goto out;

	mem = pgm_get_mem_header(uptr);
	mem->assigned_edge.graph = g - gGraphs;
	mem->assigned_edge.edge = e - g->edges;
	mem->producer_flag = is_producer;

out:
	return mem;
}

void* pgm_malloc_edge_buf_p(edge_t edge)
{
	void* mem = 0;
	struct pgm_graph* g;
	struct pgm_edge*  e;

	if(!is_valid_graph(edge.graph))
		goto out;

	g = &gGraphs[edge.graph];
	e = &g->edges[edge.edge];

	mem = pgm_malloc(e->attr.nr_produce);
out:
	return mem;
}

void* pgm_malloc_edge_buf_c(edge_t edge)
{
	void* mem = 0;
	struct pgm_graph* g;
	struct pgm_edge*  e;

	if(!is_valid_graph(edge.graph))
		goto out;

	g = &gGraphs[edge.graph];
	e = &g->edges[edge.edge];

	mem = pgm_malloc(e->attr.nr_consume);
out:
	return mem;
}

void* pgm_get_edge_buf_p(edge_t edge)
{
	void* mem = 0;
	struct pgm_graph* g;
	struct pgm_edge*  e;

	if(!is_valid_graph(edge.graph))
		goto out;

	g = &gGraphs[edge.graph];
	e = &g->edges[edge.edge];

	if(!is_data_passing(e))
	{
//		W("Requested buffer of non-data-passing edge.\n");
		goto out;
	}

	mem = pgm_get_user_ptr(e->buf_out);

out:
	return mem;
}

void* pgm_get_edge_buf_c(edge_t edge)
{
	void* mem = 0;
	struct pgm_graph* g;
	struct pgm_edge*  e;

	if(!is_valid_graph(edge.graph))
		goto out;

	g = &gGraphs[edge.graph];
	e = &g->edges[edge.edge];

	if(!is_data_passing(e))
	{
//		W("Requested buffer of non-data-passing edge.\n");
		goto out;
	}

	mem = pgm_get_user_ptr(e->buf_in);

out:
	return mem;
}

edge_t pgm_get_edge_from_buf(void* uptr)
{
	edge_t edge;
	if(is_buf_assigned(uptr, &edge))
		return edge;
	return BAD_EDGE;
}

int pgm_is_buf_in_use(void* uptr)
{
	return is_buf_assigned(uptr);
}

void* __pgm_swap_edge_buf(edge_t edge, void* new_uptr, bool swap_producer)
{
	void* old = 0;
	struct pgm_graph* g;
	struct pgm_edge*  e;
	pgm_memory_hdr_t* hdr;
	pgm_memory_hdr_t* old_hdr;
	pgm_memory_hdr_t** old_hdr_ptr;

	if(!new_uptr)
		goto out;
	if(!is_valid_graph(edge.graph))
		goto out;

	// first get the edge
	g = &gGraphs[edge.graph];
	e = &g->edges[edge.edge];

	if(!is_data_passing(e))
	{
		E("Tried to swap buffer with non-data-passing edge.\n");
		goto out;
	}

	// get the header for the new buffer
	hdr = pgm_get_mem_header_safe(new_uptr);
	if(!hdr)
	{
		E("%p is not an edge buffer pointer (or it has been corrupted)\n", new_uptr);
		goto out;
	}
	if(hdr->assigned_edge.graph != BAD_EDGE.graph &&
	   hdr->assigned_edge.edge != BAD_EDGE.edge)
	{
		E("%p is already in use by an edge.\n", new_uptr);
		goto out;
	}

	// select the old buffer
	old_hdr_ptr = (swap_producer) ? &e->buf_out : &e->buf_in;
	old_hdr = *old_hdr_ptr;
	if(hdr->usersize != old_hdr->usersize)
	{
		E("Buffer %p is the wrong size. (is %lu, expected %lu)\n",
		  new_uptr, hdr->usersize, old_hdr->usersize);
		goto out;
	}

	// swap the buffers and update headers
	hdr->assigned_edge = edge;
	hdr->producer_flag = old_hdr->producer_flag;
	old_hdr->assigned_edge = BAD_EDGE;
	*old_hdr_ptr = hdr;  // apply assignment

	// return the userpointer of the old buffer
	old = pgm_get_user_ptr(old_hdr);
out:
	return old;
}

void* pgm_swap_edge_buf_p(edge_t edge, void* new_uptr)
{
	return __pgm_swap_edge_buf(edge, new_uptr, true);
}

void* pgm_swap_edge_buf_c(edge_t edge, void* new_uptr)
{
	return __pgm_swap_edge_buf(edge, new_uptr, false);
}

int pgm_swap_edge_bufs(void* a, void* b)
{
	int ret = -1;
	struct pgm_graph *ga, *gb;
	struct pgm_edge  *ea, *eb;
	pgm_memory_hdr_t *hdra, *hdrb;
	pgm_memory_hdr_t **edgeabufptr, **edgebbufptr, *tempbufptr;
	edge_t    edgea, edgeb;
	char temp_flag;

	if(!a || !b)
		goto out;
	if(a == b)
		goto out;

	if(!is_buf_assigned(a, &edgea))
	{
		E("%p is not assigned to an edge.\n", a);
		goto out;
	}
	if(!is_buf_assigned(b, &edgeb))
	{
		E("%p is not assigned to an edge.\n", b);
		goto out;
	}

	hdra = pgm_get_mem_header(a);
	hdrb = pgm_get_mem_header(b);

	if(hdra->usersize != hdrb->usersize)
	{
		E("Buffers are not the same size: %p:%lu vs %p:%lu\n",
		  a, hdra->usersize, b, hdrb->usersize);
		goto out;
	}

	ga = &gGraphs[edgea.graph];
	ea = &ga->edges[edgea.edge];
	gb = &gGraphs[edgeb.graph];
	eb = &gb->edges[edgeb.edge];

	// update the edges first
	edgeabufptr = (hdra->producer_flag) ? &(ea->buf_out) : &(ea->buf_in);
	edgebbufptr = (hdrb->producer_flag) ? &(eb->buf_out) : &(eb->buf_in);
	tempbufptr = *edgeabufptr;
	*edgeabufptr = *edgebbufptr;
	*edgebbufptr = tempbufptr;

	// now update headers
	hdra->assigned_edge = edgeb;
	hdrb->assigned_edge = edgea;
	temp_flag = hdra->producer_flag;
	hdra->producer_flag = hdrb->producer_flag;
	hdrb->producer_flag = temp_flag;

	ret = 0;

out:
	return ret;
}

///////////////////////////////////////////////////
//           SYNC PRIMATIVE WRAPPERS             //
///////////////////////////////////////////////////

#ifdef PGM_USE_PTHREAD_SYNC

	static void pgm_lock_init(pgm_lock_t* l)
	{
		pthread_mutexattr_t attr;
		pthread_mutexattr_init(&attr);
	#ifdef PGM_SHARED
		pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	#endif
		pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT);
		pthread_mutex_init(l, &attr);
		pthread_mutexattr_destroy(&attr);
	}
	#define pgm_lock_destroy(l)  pthread_mutex_destroy((l))
	#define pgm_lock(l, flags)   do { ((void)(flags)); pthread_mutex_lock((l)); } while(0)
	#define pgm_unlock(l, flags) do { ((void)(flags)); pthread_mutex_unlock((l)); } while(0)

	static void pgm_cv_init(pgm_cv_t* cv)
	{
		pthread_condattr_t cattr;
		pthread_condattr_init(&cattr);
	#ifdef PGM_SHARED
		pthread_condattr_setpshared(&cattr, 1);
	#endif
		pthread_cond_init(cv, &cattr);
		pthread_condattr_destroy(&cattr);
	}
	#define pgm_cv_destroy(cv)   		pthread_cond_destroy((cv))
	#define pgm_cv_wait(cv, l, flags)   do{ ((void)(flags)); pthread_cond_wait((cv), (l)); } while(0)
	#define pgm_cv_signal(cv)    		pthread_cond_signal((cv))

#elif defined(PGM_USE_PGM_SYNC)

	#ifdef PGM_PREEMPTIVE
		#define pgm_lock_init(l)     spin_init((l))
		#define pgm_lock(l, flags)   do { ((void)(flags)); spin_lock((l)); } while(0)
		#define pgm_unlock(l, flags) do { ((void)(flags)); spin_unlock((l)); } while(0)
	#else
		#define pgm_lock_init(l)     spin_init_np((l))
		#define pgm_lock(l, flags)   spin_lock_np((l), &(flags))
		#define pgm_unlock(l, flags) spin_unlock_np((l), (flags))
	#endif
	#define pgm_lock_destroy(l) (void)(l)

	#ifdef PGM_PRIVATE
		#define pgm_cv_init(cv)     cv_init((cv))
	#else
		#define pgm_cv_init(cv)     cv_init_shared((cv))
	#endif
	#define pgm_cv_destroy(cv)  (void)(cv)
	#ifdef PGM_PREEMPTIVE
		#define pgm_cv_wait(cv, l, flags) do{ ((void)(flags)); cv_wait((cv), (l)); } while(0)
	#else
		#define pgm_cv_wait(cv, l, flags) cv_wait_np((cv), (l), &(flags))
	#endif
	#define pgm_cv_signal(cv)   cv_signal((cv))
#else
	#error "Unknown synchronization method."
#endif

///////////////////////////////////////////////////
//      PGM Framework Init/Destroy Routines      //
///////////////////////////////////////////////////

static int safePath(const path& dir)
{
	static const path allowed[] = {
		path("/dev/shm"),
		path("/home"),
		path("/tmp")
	};

	path dirAbs;
	if(dir.is_relative())
	{
		dirAbs = current_path();
		dirAbs /= dir;
	}
	else
	{
		dirAbs = dir;
	}

	if(dirAbs.empty() || dirAbs == path("/"))
		return -1;

	int failures = 0;
	const int nrValidStems = sizeof(allowed)/sizeof(allowed[0]);
	for(int i = 0; i < nrValidStems; ++i)
	{
		for(path::iterator compare(allowed[i].begin()), toCheck(dirAbs.begin());
			compare != allowed[i].end();
			++compare, ++toCheck)
		{
			if(*compare != *toCheck || toCheck == dirAbs.end())
			{
				++failures;
				break;
			}
		}
	}

	if(nrValidStems == failures)
		return -1;
	return 0;
}

static int prepare_dir(const path& graphDir)
{
	int ret = -1;

	if(0 != safePath(graphDir))
	{
		E("%s is an invalid path.\n", graphDir.string().c_str());
		goto out;
	}

	if(exists(graphDir) && !is_directory(graphDir))
	{
		E("%s is a file.\n", graphDir.string().c_str());
		goto out;
	}

	if(boost::filesystem::equivalent(graphDir, current_path()))
	{
		E("Current working directory cannot be the same as graph directory.\n");
		goto out;
	}

	if(!exists(graphDir))
	{
	create_dir:
		if(!create_directories(graphDir))
		{
			F("Could not create directory %s\n", graphDir.string().c_str());
			goto out;
		}
	}

	if(!filesystem::is_empty(graphDir))
	{
		if(0 == remove_all(graphDir))
		{
			F("Unable to remove files in %s\n", graphDir.string().c_str());
			goto out;
		}
		goto create_dir; // i know. this makes a child cry.
	}

	ret = 0;

out:
	return ret;
}

static string get_gMemName(const path& graphDir)
{
	const char* graphName = "gGraphSharedMem.dat";
	boost::hash<std::string> string_hash;
	stringstream ss;
	size_t hash;

	hash = string_hash(graphDir.string());
	ss<<hex<<hash<<"_"<<graphName;

	return ss.str();
}

static int prepare_graph_shared_mem(const path& graphDir)
{
	int ret = -1;

#ifdef PGM_SHARED
	string memName = get_gMemName(graphDir);

	// allocate twice as much space as we really need, just to be safe.
	size_t memsize = sizeof(struct pgm_graph) * PGM_MAX_GRAPHS * 2;

	// make sure there's nothing hanging around
	shared_memory_object::remove(memName.c_str());

	gGraphSharedMem = new managed_shared_memory(create_only, memName.c_str(), memsize);
	if(!gGraphSharedMem)
	{
		F("Could not create shared memory file %s\n", memName.c_str());
		goto out;
	}

	gGraphs = gGraphSharedMem->construct<struct pgm_graph>("struct pgm_graph gGraphs")[PGM_MAX_GRAPHS]();
	if(!gGraphs)
	{
		F("Shared memory allocation failure.\n");
		goto out;
	}
	memset(gGraphs, 0, sizeof(struct pgm_graph)*PGM_MAX_GRAPHS);

	ret = 0;
	gIsGraphMaster = true;
	gMemName = memName;
out:
#else
	F("PGM not compiled with shared mem support.\n");
#endif
	return ret;
}

static int open_graph_shared_mem(const path& graphDir, const int timeout_s = 60)
{
	int ret = -1;
#ifdef PGM_SHARED
	int time = 0;
	string memName = get_gMemName(graphDir);

	do
	{
		try
		{
			if(!gGraphSharedMem)
				gGraphSharedMem = new managed_shared_memory(open_only,memName.c_str());
		}
		catch (...)
		{
			sleep(1);

			if(timeout_s == ++time)
				goto out;
		}
	} while (!gGraphSharedMem);

	gGraphs = gGraphSharedMem->find<struct pgm_graph>("struct pgm_graph gGraphs").first;
	ret = 0;
out:
#else
	F("PGM not compiled with shared memory support.\n");
#endif
	return ret;
}

static int prepare_graph_private_mem(void)
{
	int ret = -1;

	if (gGraphs == 0)
	{
		gGraphs = new (std::nothrow) struct pgm_graph[PGM_MAX_GRAPHS];
		if (gGraphs)
		{
			memset(gGraphs, 0, sizeof(struct pgm_graph)*PGM_MAX_GRAPHS);
			gIsGraphMaster = true;
			ret = 0;
		}
	}
	else
	{
		ret = 1; // TODO: establish proper PGM error codes
	}

	return ret;
}

int pgm_init_process_local(void)
{
	int ret = prepare_graph_private_mem();
	return ret;
}

int pgm_init1(const char* dir){
	return pgm_init3(dir, 0, 0);
}
int pgm_init2(const char* dir, int create){
	return pgm_init3(dir, create, 0);
}
int pgm_init3(const char* dir, int create, int use_shared_mem)
{
	int ret = -1;
	path graphDir(dir);

	if(graphDir.is_relative())
	{
		graphDir = current_path();
		graphDir /= dir;
	}

	if(create)
	{
		// TODO: Create dir for FIFOs on demand.
		ret = prepare_dir(graphDir);
		if(0 != ret)
			goto out;

		if(use_shared_mem)
		{
			ret = prepare_graph_shared_mem(graphDir);
			if(0 != ret)
				goto out;
		}
		else
		{
			ret = prepare_graph_private_mem();
		}
	}
	else
	{
		if(!use_shared_mem)
			goto out;

		ret = open_graph_shared_mem(graphDir);
		if(0 != ret)
			goto out;
	}

	gGraphPath = graphDir;

out:
	return ret;
}

int pgm_destroy(void)
{
	int ret = -1;

	if(gGraphSharedMem)
	{
		// we're shared memory
		gGraphs = 0;
		delete gGraphSharedMem;
		gGraphSharedMem = 0;

		if(gIsGraphMaster)
			shared_memory_object::remove(gMemName.c_str());

		ret = 0;
	}
	else
	{
		// we (might) be private memory
		if(gGraphs)
		{
			delete [] gGraphs;
			gGraphs = 0;
			ret = 0;
		}
	}

	return ret;
}


///////////////////////////////////////////////////
//     Graph/Node/Edge Init/Destroy Routines     //
///////////////////////////////////////////////////

static int prepare_graph(graph_t* graph, const char* graph_name)
{
	int ret = -1;
	struct pgm_graph *g;
	size_t len;

	*graph = -1;
	for(int i = 0; i < PGM_MAX_GRAPHS; ++i)
	{
		if(!gGraphs[i].in_use)
		{
			*graph = i;
			break;
		}
	}

	if(*graph == -1)
	{
		E("Out of graph slots\n");
		goto out;
	}

	g = &gGraphs[*graph];
	memset(g, 0, sizeof(struct pgm_graph));
	g->in_use = 1;

	len = strnlen(graph_name, PGM_GRAPH_NAME_LEN);
	if(len <= 0 || len > PGM_GRAPH_NAME_LEN)
	{
		E("Bad graph name length: %d\n", (int)len);
		goto out;
	}

	strncpy(g->name, graph_name, PGM_GRAPH_NAME_LEN);

	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
#ifdef PGM_SHARED
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
#endif
	pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT);
	pthread_mutex_init(&g->lock, &attr);
	pthread_mutexattr_destroy(&attr);

	ret = 0;

out:
	return ret;
}

static int open_graph(graph_t* graph, const char* graph_name)
{
	size_t len = strlen(graph_name);

	*graph = -1;
	for(int i = 0; i < PGM_MAX_GRAPHS; ++i)
	{
		if(0 == strncmp(gGraphs[i].name, graph_name, len))
		{
			*graph = i;
			break;
		}
	}

	return (*graph == -1) ? -1: 0;
}

static void __destroy_graph(struct pgm_graph* g)
{
	for(int i = 0; i < g->nr_edges; ++i)
	{
		g->edges[i].ops->destroy(g,
				&(g->nodes[g->edges[i].producer]),
				&(g->nodes[g->edges[i].consumer]),
				&(g->edges[i]));
	}

	for(int i = 0; i < g->nr_nodes; ++i)
	{
		pgm_cv_destroy(&g->nodes[i].wait);
		pgm_lock_destroy(&g->nodes[i].lock);
	}

	g->in_use = 0;
	g->nr_nodes = 0;
	memset(g->name, 0, sizeof(g->name));
	memset(g->nodes, 0, sizeof(g->nodes));
}

int pgm_destroy_graph(graph_t graph)
{
	int ret = -1;
	int abort = 0;
	struct pgm_graph* g;

	if(!gIsGraphMaster)
		goto out;
	if(!is_valid_graph(graph))
		goto out;

	g = &gGraphs[graph];


	pthread_mutex_lock(&g->lock);
	for(int i = 0; i < g->nr_nodes; ++i)
	{
		if(g->nodes[i].owner != UNCLAIMED_NODE)
		{
			E("Node %s still in use by %d\n", g->nodes[i].name, g->nodes[i].owner);
			abort = 1;
			goto out_unlock;
		}
	}

out_unlock:
	if(!abort)
	{
		__destroy_graph(g);
		pthread_mutex_unlock(&g->lock);
		ret = 0;
	}
	else
	{
		pthread_mutex_unlock(&g->lock);
	}

out:
	return ret;
}

int pgm_init_graph(graph_t* graph, const char* graph_name)
{
	int ret = -1;
	size_t len;

	if(!gGraphs)
		goto out;

	len = strnlen(graph_name, PGM_GRAPH_NAME_LEN);
	if(len <= 0 || len > PGM_GRAPH_NAME_LEN)
		goto out;

	if(gIsGraphMaster && 0 != pgm_find_graph(graph, graph_name))
		ret = prepare_graph(graph, graph_name);
out:
	return ret;
}

int pgm_init_graph_int(graph_t* graph, unsigned int numerical_name)
{
	char name[PGM_GRAPH_NAME_LEN];
	snprintf(name, PGM_GRAPH_NAME_LEN, "%x", numerical_name);
	return pgm_init_graph(graph, name);
}

int pgm_init_node(node_t* node, graph_t graph, const char* name)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;
	size_t len;

	if(!node || !is_valid_graph(graph))
		goto out;
	if(!gIsGraphMaster)
		goto out;
	len = strnlen(name, PGM_NODE_NAME_LEN);
	if(len <= 0 || len > PGM_NODE_NAME_LEN)
		goto out;

	g = &gGraphs[graph];
	pthread_mutex_lock(&g->lock);

	if(g->nr_nodes + 1 == PGM_MAX_NODES)
	{
		E("No more available nodes for graph %s.\n", g->name);
		goto out_unlock;
	}

	node->graph = graph;
	node->node = (g->nr_nodes)++;
	n = &g->nodes[node->node];

	// memset just to be safe...
	memset(n, 0, sizeof(*n));
	n->owner = UNCLAIMED_NODE;
	strncpy(n->name, name, len);

	pgm_lock_init(&n->lock);
	pgm_cv_init(&n->wait);

	ret = 0;

out_unlock:
	pthread_mutex_unlock(&g->lock);
out:
	return ret;
}

int pgm_init_node_int(node_t* node, graph_t graph, unsigned int numerical_name)
{
	char name[PGM_NODE_NAME_LEN];
	snprintf(name, PGM_NODE_NAME_LEN, "%x", numerical_name);
	return pgm_init_node(node, graph, name);
}

static int __pgm_init_edge(edge_t* edge,
	node_t producer, node_t consumer, const char* name,
	const edge_attr_t* attr,
	bool is_backedge, size_t nr_skips)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_edge* e;
	struct pgm_node* np;
	struct pgm_node* nc;
	size_t len;

	if(	!edge ||
		(producer.graph != consumer.graph) ||
		!is_valid_graph(producer.graph) )
		goto out;
	if(!gIsGraphMaster)
		goto out;
	len = strnlen(name, PGM_EDGE_NAME_LEN);
	if(len <= 0 || len > PGM_EDGE_NAME_LEN)
		goto out;

	if((attr->type & __PGM_EDGE_MQ) && (attr->nr_produce != attr->nr_consume))
	{
		E("Produce amnt. must equal consume amnt. for POSIX msg queues.\n");
		goto out;
	}
	if(attr->type & __PGM_EDGE_RING)
	{
		if(attr->nr_produce != attr->nr_consume)
		{
			E("Produce amnt. must equal consume amnt. for ring buffers.\n");
			goto out;
		}
		if(attr->nmemb == 0)
		{
			E("nmemb cannot be zero.\n");
			goto out;
		}
	}

	if(attr->nr_threshold < attr->nr_consume)
		goto out;
	if(attr->nr_produce <= 0 || attr->nr_consume <= 0 || attr->nr_threshold <= 0)
		goto out;
	if(attr->type == 0)
		goto out;

	g = &gGraphs[producer.graph];
	pthread_mutex_lock(&g->lock);

	if(g->nr_edges + 1 == PGM_MAX_EDGES)
	{
		E("No more available edges for graph %s.\n", g->name);
		goto out_unlock;
	}
	if(g->nr_nodes <= producer.node || g->nr_nodes <= consumer.node)
	{
		E("Invalid nodes.\n");
		goto out_unlock;
	}

	edge->graph = producer.graph;
	edge->edge = (g->nr_edges)++;
	e = &g->edges[edge->edge];

	np = &g->nodes[producer.node];
	if(np->nr_out+1 == PGM_MAX_OUT_DEGREE)
		goto out_unlock;
	nc = &g->nodes[consumer.node];
	if(nc->nr_in+1 == PGM_MAX_IN_DEGREE)
		goto out_unlock;

	if(is_signal_driven(attr))
	{
		nc->nr_in_signaled++;
		nc->signal_edge_mask |= ((pgm_fd_mask_t)(1))<<(nc->nr_in);

		if(is_backedge)
		{
			nc->nr_in_signaled_backedges++;
		}
	}
	if(is_data_passing(attr))
	{
		nc->nr_in_data++;
		if(is_backedge)
		{
			nc->nr_in_data_backedges++;
		}
	}

	np->out[np->nr_out++] = edge->edge;
	nc->in[nc->nr_in++] = edge->edge;

	// memset just to be safe...
	memset(e, 0, sizeof(*e));
	strncpy(e->name, name, len);
	e->producer = producer.node;
	e->consumer = consumer.node;
	e->attr = *attr;

	if(is_backedge)
	{
		e->is_backedge = true;
		e->nr_skips = nr_skips;
	}

	if     (attr->type & __PGM_EDGE_CV)
		e->ops = &pgm_cv_edge_ops;
	else if(attr->type & __PGM_EDGE_FIFO)
		e->ops = &pgm_fifo_edge_ops;
	else if(attr->type & __PGM_EDGE_MQ)
		e->ops = &pgm_mq_edge_ops;
	else if(attr->type & __PGM_EDGE_RING)
		e->ops = &pgm_ring_edge_ops;
	else if(attr->type & __PGM_EDGE_SOCK_STREAM)
		e->ops = &pgm_sock_stream_edge_ops;
	else
		goto out_unlock;

	ret = e->ops->init(g, np, nc, e);

out_unlock:
	pthread_mutex_unlock(&g->lock);
out:
	return ret;
}

int pgm_init_edge4(edge_t* edge, node_t producer, node_t consumer,
	const char* name){
		printf("init\n");
		return pgm_init_edge5(edge, producer, consumer, name, &default_edge);
	}
int pgm_init_edge5(edge_t* edge,
	node_t producer, node_t consumer, const char* name,
	const edge_attr_t* attr)
{
	int ret = __pgm_init_edge(edge, producer, consumer, name, attr, false, 0);
	return ret;
}

int pgm_init_edge_int5(edge_t* edge, node_t producer, node_t consumer,
	unsigned int numerical_name,
	const edge_attr_t* attrs)
{
	char name[PGM_EDGE_NAME_LEN];
	snprintf(name, PGM_EDGE_NAME_LEN, "%x", numerical_name);
	return pgm_init_edge5(edge, producer, consumer, name, attrs);
}

int pgm_init_edge_int4(edge_t* edge, node_t producer, node_t consumer,
	unsigned int numerical_name){
		return pgm_init_edge_int5(edge, producer, consumer, numerical_name, &default_edge);
	}

int pgm_init_backedge5(edge_t* edge, size_t nr_skips,
  	node_t producer, node_t consumer,
  	const char* name){
			return pgm_init_backedge6(edge, nr_skips, producer, consumer, name, &default_edge);
		}

int pgm_init_backedge6(edge_t* edge, size_t nr_skips,
	node_t producer, node_t consumer, const char* name,
	const edge_attr_t* attr)
{
	int ret = __pgm_init_edge(edge, producer, consumer, name, attr, true, nr_skips);
	return ret;
}

int pgm_init_backedge_int5(edge_t* edge, size_t nskips,
		node_t producer, node_t consumer,
		unsigned int numerical_name){
			return pgm_init_backedge_int6(edge, nskips, producer, consumer, numerical_name, &default_edge);
		}

int pgm_init_backedge_int6(edge_t* edge, size_t nr_skips,
	node_t producer, node_t consumer,
	unsigned int numerical_name,
	const edge_attr_t* attrs)
{
	char name[PGM_EDGE_NAME_LEN];
	snprintf(name, PGM_EDGE_NAME_LEN, "%x", numerical_name);
	return pgm_init_backedge6(edge, nr_skips, producer, consumer, name, attrs);
}

///////////////////////////////////////////////////
//        Graph/Node/Edge Query Routines         //
///////////////////////////////////////////////////

int pgm_find_graph(graph_t* graph, const char* graph_name)
{
	int ret = -1;
	size_t len;

	if(!gGraphSharedMem)
		goto out;

	len = strnlen(graph_name, PGM_GRAPH_NAME_LEN);
	if(len <= 0 || len > PGM_GRAPH_NAME_LEN)
		goto out;

	ret = open_graph(graph, graph_name);

out:
	return ret;
}

int pgm_find_graph_int(graph_t* graph, unsigned int numerical_name)
{
	char name[PGM_GRAPH_NAME_LEN];
	snprintf(name, PGM_GRAPH_NAME_LEN, "%x", numerical_name);
	return pgm_find_graph(graph, name);
}

int pgm_find_node(node_t* node, graph_t graph, const char* name)
{
	int ret = -1;
	size_t len;
	struct pgm_graph* g;

	if(!node || !is_valid_graph(graph))
		goto out;
	len = strnlen(name, PGM_NODE_NAME_LEN);
	if(len <= 0 || len > PGM_NODE_NAME_LEN)
		goto out;

	g = &gGraphs[graph];

	pthread_mutex_lock(&g->lock);
	for(int i = 0; i < g->nr_nodes; ++i)
	{
		if(0 == strncmp(g->nodes[i].name, name, len))
		{
			node->graph = graph;
			node->node = i;
			ret = 0;
			break;
		}
	}
	pthread_mutex_unlock(&g->lock);

out:
	return ret;
}

int pgm_find_node_int(node_t* node, graph_t graph, unsigned int numerical_name)
{
	char name[PGM_NODE_NAME_LEN];
	snprintf(name, PGM_NODE_NAME_LEN, "%x", numerical_name);
	return pgm_find_node(node, graph, name);
}

int pgm_find_edge4(edge_t* edge, node_t producer, node_t consumer,
 const char* name){
	 return pgm_find_edge5(edge, producer, consumer, name, NULL);
 }

int pgm_find_edge5(edge_t* edge, node_t producer, node_t consumer,
				const char* name, edge_attr_t* attr)
{
	int ret = -1;
	struct pgm_graph* g;
	size_t len;

	if(!name)
		return pgm_find_first_edge3(edge, producer, consumer);

	if(	!edge ||
		(producer.graph != consumer.graph) ||
		!is_valid_graph(producer.graph))
		goto out;
	len = strnlen(name, PGM_EDGE_NAME_LEN);
	if(len <= 0 || len > PGM_EDGE_NAME_LEN)
		goto out;

	g = &gGraphs[producer.graph];

	pthread_mutex_lock(&g->lock);
	for(int i = 0; i < g->nr_edges; ++i)
	{
		if(g->edges[i].producer == producer.node &&
		   g->edges[i].consumer == consumer.node &&
		   (0 == strncmp(g->edges[i].name, name, len)))
		{
			int found = 0;
			pgm_node *np = &g->nodes[producer.node];
			pgm_node *nc = &g->nodes[consumer.node];

			for(int j = 0; j < np->nr_out; ++j)
			{
				if(i == np->out[j])
				{
					found++;
					break;
				}
			}
			for(int j = 0; j < nc->nr_in; ++j)
			{
				if(i == nc->in[j])
				{
					found++;
					break;
				}
			}

			if(found != 2)
				goto out_unlock;

			if(attr != 0)
				*attr = g->edges[i].attr;
			edge->graph = producer.graph;
			edge->edge = i;
			ret = 0;
			break;
		}
	}

out_unlock:
	pthread_mutex_unlock(&g->lock);
out:
	return ret;
}
int pgm_find_edge_int4(edge_t* edge, node_t producer, node_t consumer,
		unsigned int numerical_name){
			return pgm_find_edge_int5(edge, producer, consumer, numerical_name, NULL);
		}

int pgm_find_edge_int5(edge_t* edge, node_t producer, node_t consumer,
	unsigned int numerical_name,
	edge_attr_t* attrs)
{
	char name[PGM_EDGE_NAME_LEN];
	snprintf(name, PGM_EDGE_NAME_LEN, "%x", numerical_name);
	return pgm_find_edge5(edge, producer, consumer, name, attrs);
}

int pgm_find_first_edge3(edge_t* edge, node_t producer, node_t consumer){
	return pgm_find_first_edge4(edge, producer, consumer, NULL);
}


int pgm_find_first_edge4(edge_t* edge, node_t producer, node_t consumer, edge_attr_t *attr)
{
	int ret = -1;
	struct pgm_graph* g;

	if(!edge ||
		(producer.graph != consumer.graph) ||
		!is_valid_graph(producer.graph))
		goto out;

	g = &gGraphs[producer.graph];

	pthread_mutex_lock(&g->lock);
	for(int i = 0; i < g->nr_edges; ++i)
	{
		if(g->edges[i].producer == producer.node &&
		   g->edges[i].consumer == consumer.node)
		{
			if(attr != 0)
				*attr = g->edges[i].attr;
			edge->graph = producer.graph;
			edge->edge = i;
			ret = 0;
			break;
		}
	}
	pthread_mutex_unlock(&g->lock);

out:
	return ret;
}

int pgm_set_user_data(node_t node, void* udata)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	n->userdata = udata;
	ret = 0;

out:
	return ret;
}

void* pgm_get_user_data(node_t node)
{
	void* udata = 0;
	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	udata = n->userdata;

out:
	return udata;
}
int pgm_get_successors2(node_t n, node_t* successors, int len){
	return pgm_get_successors3(n, successors, len, 1);
}

int pgm_get_successors3(node_t node, node_t* successors, int len, int ignore_backedges)
{
	int num = -1;
	struct pgm_graph* g;
	struct pgm_node* n;
	node_t* step = successors;

	if(!successors || !is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	pthread_mutex_lock(&g->lock);

	if(len < n->nr_out)
		goto out_unlock;
	num = n->nr_out;

	for(int i = 0, nr_to_check = num; i < nr_to_check; ++i)
	{
		if(ignore_backedges && g->edges[n->out[i]].is_backedge)
		{
			--num;
		}
		else
		{
			const pgm_node* const _succ = &g->nodes[g->edges[n->out[i]].consumer];
			node_t succ =
			{
				.graph = node.graph,
				.node = (int)(_succ - &g->nodes[0])
			};
			*step = succ;
			++step;
		}
	}
out_unlock:
	pthread_mutex_unlock(&g->lock);
out:
	return num;
}

int pgm_get_edges_out2(node_t n, edge_t* edges, int len){
	return pgm_get_edges_out3(n, edges, len, 1);
}

int pgm_get_edges_out3(node_t node, edge_t* edges, int len, int ignore_backedges)
{
	int num = -1;
	struct pgm_graph* g;
	struct pgm_node* n;
	edge_t* step = edges;

	if(!edges || !is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	pthread_mutex_lock(&g->lock);

	if(len < n->nr_out)
		goto out_unlock;
	num = n->nr_out;

	for(int i = 0, nr_to_check = num; i < nr_to_check; ++i)
	{
		if(ignore_backedges && g->edges[n->out[i]].is_backedge)
		{
			--num;
		}
		else
		{
			edge_t e =
			{
				.graph = node.graph,
				.edge = n->out[i]
			};
			*step = e;
			++step;
		}
	}
out_unlock:
	pthread_mutex_unlock(&g->lock);
out:
	return num;
}

int pgm_get_predecessors2(node_t node, node_t* predecessors, int len){
	return pgm_get_predecessors3(node, predecessors, len, 1);
}

int pgm_get_predecessors3(node_t node, node_t* predecessors, int len, int ignore_backedges)
{
	int num = -1;
	struct pgm_graph* g;
	struct pgm_node* n;
	node_t* step = predecessors;

	if(!predecessors || !is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	pthread_mutex_lock(&g->lock);

	if(len < n->nr_in)
		goto out_unlock;
	num = n->nr_in;

	for(int i = 0, nr_to_check = num; i < nr_to_check; ++i)
	{
		if(ignore_backedges && g->edges[n->in[i]].is_backedge)
		{
			--num;
		}
		else
		{
			const pgm_node* const _pred = &g->nodes[g->edges[n->in[i]].producer];
			node_t pred =
			{
				.graph = node.graph,
				.node = (int)(_pred - &g->nodes[0])
			};
			*step = pred;
			++step;
		}
	}
out_unlock:
	pthread_mutex_unlock(&g->lock);
out:
	return num;
}

int pgm_get_edges_in3(node_t n, edge_t* edges, int len){
	return pgm_get_edges_in4(n, edges, len, 1);
}

int pgm_get_edges_in4(node_t node, edge_t* edges, int len, int ignore_backedges)
{
	int num = -1;
	struct pgm_graph* g;
	struct pgm_node* n;
	edge_t* step = edges;

	if(!edges || !is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	pthread_mutex_lock(&g->lock);

	if(len < n->nr_in)
		goto out_unlock;
	num = n->nr_in;

	for(int i = 0, nr_to_check = num; i < nr_to_check; ++i)
	{
		if(ignore_backedges && g->edges[n->in[i]].is_backedge)
		{
			--num;
		}
		else
		{
			edge_t e =
			{
				.graph = node.graph,
				.edge = n->in[i]
			};
			*step = e;
			++step;
		}
	}
out_unlock:
	pthread_mutex_unlock(&g->lock);
out:
	return num;
}

const char* pgm_get_name(node_t node)
{
	const char* name = 0;

	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];
	name = n->name;

out:
	return name;
}

node_t pgm_get_producer(edge_t edge)
{
	node_t n = {edge.graph, -1};
	struct pgm_graph* g;
	struct pgm_edge* e;
	if(!is_valid_graph(edge.graph))
		goto out;
	g = &gGraphs[edge.graph];
	e = &g->edges[edge.edge];
	n.node = e->producer;
out:
	return n;
}

node_t pgm_get_consumer(edge_t edge)
{
	node_t n = {edge.graph, -1};
	struct pgm_graph* g;
	struct pgm_edge* e;
	if(!is_valid_graph(edge.graph))
		goto out;
	g = &gGraphs[edge.graph];
	e = &g->edges[edge.edge];
	n.node = e->consumer;
out:
	return n;
}

int pgm_get_edge_attrs(edge_t edge, edge_attr_t* attrs)
{
	int ret = -1;
	struct pgm_graph* g;

	if(!is_valid_graph(edge.graph))
		goto out;
	if(!attrs)
		goto out;

	g = &gGraphs[edge.graph];
	*attrs = g->edges[edge.edge].attr;

out:
	return ret;
}

int pgm_is_backedge(edge_t backedge)
{
	int is_be = 0;
	struct pgm_graph* g;

	if(!is_valid_graph(backedge.graph))
		goto out;

	g = &gGraphs[backedge.graph];
	is_be = g->edges[backedge.edge].is_backedge;

out:
	return is_be;
}

size_t pgn_get_nr_skips_remaining(edge_t backedge)
{
	size_t remaining = 0;
	struct pgm_graph* g;

	if(!is_valid_graph(backedge.graph))
		goto out;

	g = &gGraphs[backedge.graph];
	if(g->edges[backedge.edge].is_backedge)
		remaining = g->edges[backedge.edge].nr_skips;

out:
	return remaining;
}

int pgm_get_nr_produce(edge_t edge)
{
	int produced = -1;
	struct pgm_graph* g;

	if(!is_valid_graph(edge.graph))
		goto out;

	g = &gGraphs[edge.graph];
	produced = g->edges[edge.edge].attr.nr_produce;

out:
	return produced;
}

int pgm_get_nr_consume(edge_t edge)
{
	int consumed = -1;
	struct pgm_graph* g;

	if(!is_valid_graph(edge.graph))
		goto out;

	g = &gGraphs[edge.graph];
	consumed = g->edges[edge.edge].attr.nr_consume;

out:
	return consumed;
}

int pgm_get_nr_threshold(edge_t edge)
{
	int threshold = -1;
	struct pgm_graph* g;

	if(!is_valid_graph(edge.graph))
		goto out;

	g = &gGraphs[edge.graph];
	threshold = g->edges[edge.edge].attr.nr_threshold;

out:
	return threshold;
}

int pgm_get_degree1(node_t node){
	return pgm_get_degree2(node, 1);
}

int pgm_get_degree2(node_t node, int ignore_backedges)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	pthread_mutex_lock(&g->lock);
	if(!ignore_backedges)
	{
		ret = n->nr_in + n->nr_out;
	}
	else
	{
		ret = 0;
		for(int i = 0; i < n->nr_in; ++i)
		{
			if(!g->edges[n->in[i]].is_backedge)
				++ret;
		}
		for(int i = 0; i < n->nr_out; ++i)
		{
			if(!g->edges[n->out[i]].is_backedge)
				++ret;
		}
	}
	pthread_mutex_unlock(&g->lock);

out:
	return ret;
}

int pgm_get_degree_in1(node_t node){
	return pgm_get_degree_in2(node, 1);
}

int pgm_get_degree_in2(node_t node, int ignore_backedges)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	pthread_mutex_lock(&g->lock);
	if(!ignore_backedges)
	{
		ret = n->nr_in;
	}
	else
	{
		ret = 0;
		for(int i = 0; i < n->nr_in; ++i)
		{
			if(!g->edges[n->in[i]].is_backedge)
				++ret;
		}
	}
	pthread_mutex_unlock(&g->lock);

out:
	return ret;
}

int pgm_get_degree_out1(node_t node){
	return pgm_get_degree_out2(node, 1);
};

int pgm_get_degree_out2(node_t node, int ignore_backedges)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	pthread_mutex_lock(&g->lock);
	if(!ignore_backedges)
	{
		ret = n->nr_out;
	}
	else
	{
		ret = 0;
		for(int i = 0; i < n->nr_out; ++i)
		{
			if(!g->edges[n->out[i]].is_backedge)
				++ret;
		}
	}
	pthread_mutex_unlock(&g->lock);

out:
	return ret;
}

int pgm_is_parent(node_t node, node_t query)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;
	struct pgm_node* q;

	if(node.graph != query.graph)
		goto out;
	if(!is_valid_graph(node.graph))
		goto out;

	ret = 0;
	if(node.node == query.node)
	{
		goto out;
	}

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];
	q = &g->nodes[query.node];

	pthread_mutex_lock(&g->lock);
	for(int i = 0; i < n->nr_in; ++i)
	{
		if(g->edges[n->in[i]].is_backedge)
		{
			continue; // skip backedge
		}

		if(q == &g->nodes[g->edges[n->in[i]].producer])
		{
			ret = 1;
			break;
		}
	}
	pthread_mutex_unlock(&g->lock);

out:
	return ret;
}

int pgm_is_child(node_t node, node_t query)
{
	return pgm_is_parent(query, node);
}

static int is_ancestor(const struct pgm_graph* g,
	const struct pgm_node* n, const struct pgm_node* q,
	std::set<int>& visited)
{
	if(n == q)
		return 1;

	const int degree = n->nr_in;
	if(degree == 0)
		return 0;

	int is_pred = 0;

	// depth-first search to see if q is a predecessor of n.
	for(int i = 0; i < degree && !is_pred; ++i)
	{
		if(g->edges[n->in[i]].is_backedge)
			continue; // skip backeges

		int parent_idx = g->edges[n->in[i]].producer;
		if(visited.find(parent_idx) == visited.end())
		{
			visited.insert(parent_idx);

			const struct pgm_node* parent = &g->nodes[parent_idx];
			is_pred = is_ancestor(g, parent, q, visited);
		}
	}

	return is_pred;
}

int pgm_is_ancestor(node_t node, node_t query)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;
	struct pgm_node* q;

	if(node.graph != query.graph)
		goto out;
	if(!is_valid_graph(node.graph))
		goto out;
	if(node.node == query.node)
	{
		ret = 0;
		goto out;
	}
	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];
	q = &g->nodes[query.node];

	{
		std::set<int> visited;

		pthread_mutex_lock(&g->lock);
		ret = is_ancestor(g, n, q, visited);
		pthread_mutex_unlock(&g->lock);
	}

out:
	return ret;
}

int pgm_is_descendant(node_t n, node_t query)
{
	return pgm_is_ancestor(query, n);
}

///////////////////////////////////////////////////
//           Graph Validation Routines           //
///////////////////////////////////////////////////

static int dag_visit(
	const struct pgm_graph* const g,
	const struct pgm_node* const n,
	std::set<std::string>& visited,
	std::set<std::string>& path,
	int ignore_explicit_backedges)
{
	const std::string name(n->name);

	// recursive DFS to detect cycles
	if(visited.find(name) == visited.end())
	{
		visited.insert(name);
		path.insert(name);

		for(int i = 0; i < n->nr_out; ++i)
		{
			if(ignore_explicit_backedges && g->edges[n->out[i]].is_backedge)
			{
				continue;
			}

			const struct pgm_node* const successor =
					&(g->nodes[g->edges[n->out[i]].consumer]);
			const std::string successor_name(successor->name);

			// already appears on this path?
			if(path.find(successor_name) != path.end())
				return 0;

			// visit successor
			if(!dag_visit(g, successor, visited, path, ignore_explicit_backedges))
				return 0;
		}

		path.erase(path.find(name));
	}

	return 1;
}
int pgm_is_dag1(graph_t graph){
	return pgm_is_dag2(graph, 1);
}

int pgm_is_dag2(graph_t graph, int ignore_explicit_backedges)
{
	int isDag = 1; // assume true
	if(!is_valid_graph(graph))
	{
		isDag = 0;
	}
	else
	{
		const struct pgm_graph* const g = &gGraphs[graph];
		std::set<std::string> visited;

		// there might be multiple roots or even unconnected nodes,
		// so iterate over the set until all have been visited or
		// graph proven not to be a dag.
		for(int i = 0; i < g->nr_nodes && 1 == isDag; ++i)
		{
			const pgm_node* const n = &(g->nodes[i]);
			if(visited.find(std::string(n->name)) == visited.end())
			{
				std::set<std::string> path;
				isDag = dag_visit(g, n, visited, path, ignore_explicit_backedges);
			}
		}
	}

	return isDag;
}


///////////////////////////////////////////////////
//        Longest/Shortest Path Routines         //
///////////////////////////////////////////////////

// We use boost's graph library (BGL) to compute longest and
// shortest paths. In the future, we may just want to port
// the entire PGM graph structures to BGL. Alternatively, we
// may want to explore using BGL's adaptors to map PGM's graph
// structures to a BGL interface. However, for now, we translate
// between PGM's internal graph structures and BGL on demand,
// since BGL is incredibly complex and has a very steep learning
// curve. This is not efficient, but these longest/shortest routines
// should only be called during an initialization phase, so
// speed is not terribly important.

typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS,
		boost::no_property, boost::property<boost::edge_weight_t,
		double> > bgraph_t;
typedef boost::graph_traits<bgraph_t>::vertex_descriptor bnode_t;
typedef std::pair<int, int> bedge_t;

double pgm_get_max_depth1(node_t node){
	return pgm_get_max_depth3(node, NULL, NULL);
}

double pgm_get_max_depth2(node_t node, pgm_weight_func_t w){
	return pgm_get_max_depth3(node, w, NULL);
}


double pgm_get_max_depth3(node_t target, pgm_weight_func_t wfunc, void* user)
{
	// We can use BGL to compute the longest path by negating
	// edge weights. This is safe since the input gGraphs are
	// are all DAGs.

	double dist = -1.0;
	struct pgm_graph* g;

	if(!is_valid_graph(target.graph))
		goto out;
	if(!pgm_is_dag2(target.graph, true))
		goto out;
	if(target.node < 0)
		goto out;

	g = &gGraphs[target.graph];

	pthread_mutex_lock(&g->lock);
	{
		if(target.node >= g->nr_nodes)
		{
			pthread_mutex_unlock(&g->lock);
			goto out;
		}

		int nr_foward_edges = 0;
		for(int i = 0; i < g->nr_edges; ++i)
		{
			if(g->edges[i].is_backedge)
				continue;
			++nr_foward_edges;
		}

		bedge_t* edge_array = new bedge_t[nr_foward_edges];
		double* weights = new double[nr_foward_edges];
		for(int i = 0, j = 0; i < g->nr_edges; ++i)
		{
			if(g->edges[i].is_backedge)
				continue;

			edge_array[j++] =
				std::make_pair(g->edges[i].producer, g->edges[i].consumer);
		}

		if(wfunc)
		{
			for(int i = 0, j = 0; i < g->nr_edges; ++i)
			{
				if(g->edges[i].is_backedge)
					continue;

				edge_t external_rep = {target.graph, i};
				weights[j++] = -1.0*wfunc(external_rep, user);
			}
		}
		else
		{
			std::fill(weights, weights + nr_foward_edges, -1.0);
		}

		bgraph_t bgraph(edge_array, edge_array + nr_foward_edges,
						weights, g->nr_nodes);
		std::vector<bnode_t> p(boost::num_vertices(bgraph));
		std::vector<double> d(boost::num_vertices(bgraph));

		dist = std::numeric_limits<double>::max();
		for(int i = 0; i < g->nr_nodes; ++i)
		{
			int nr_forward_in = 0;
			for(int j = 0; j < g->nodes[i].nr_in; ++j)
			{
				if(g->edges[g->nodes[i].in[j]].is_backedge)
					continue;
				nr_forward_in++;
			}
			if(nr_forward_in != 0)
				continue;
			std::fill(d.begin(), d.end(), std::numeric_limits<double>::max());
			d[i] = 0.0;
			boost::bellman_ford_shortest_paths(bgraph,
							boost::num_vertices(bgraph),
							boost::predecessor_map(&p[0]).distance_map(&d[0]));
			double adist = d[target.node];
			if(adist > 0.0)
				adist = std::numeric_limits<double>::max();
			dist = std::min(dist, adist);
		}

		delete edge_array;
		delete weights;
	}
	pthread_mutex_unlock(&g->lock);

	dist *= -1.0; // negate the distance to get positive distance
out:
	return dist;
}

double pgm_get_min_depth1(node_t node){
	return pgm_get_min_depth3(node, NULL, NULL);
}

double pgm_get_min_depth2(node_t node, pgm_weight_func_t w){
	return pgm_get_min_depth3(node, w, NULL);
}

double pgm_get_min_depth3(node_t target, pgm_weight_func_t wfunc, void* user)
{
	double dist = -1;
	struct pgm_graph* g;

	if(!is_valid_graph(target.graph))
		goto out;
	if(!pgm_is_dag2(target.graph, true))
		goto out;
	if(target.node < 0)
		goto out;

	g = &gGraphs[target.graph];

	pthread_mutex_lock(&g->lock);
	{
		if(target.node >= g->nr_nodes)
		{
			pthread_mutex_unlock(&g->lock);
			goto out;
		}

		int nr_foward_edges = 0;
		for(int i = 0; i < g->nr_edges; ++i)
		{
			if(g->edges[i].is_backedge)
				continue;
			++nr_foward_edges;
		}

		bedge_t* edge_array = new bedge_t[nr_foward_edges];
		double* weights = new double[nr_foward_edges];
		for(int i = 0, j = 0; i < g->nr_edges; ++i)
		{
			if(g->edges[i].is_backedge)
				continue;
			edge_array[j++] =
				std::make_pair(g->edges[i].producer, g->edges[i].consumer);
		}
		if(wfunc)
		{
			for(int i = 0, j = 0; i < g->nr_edges; ++i)
			{
				if(g->edges[i].is_backedge)
					continue;

				edge_t external_rep = {target.graph, i};
				weights[j++] = wfunc(external_rep, user);
			}
		}
		else
		{
			std::fill(weights, weights + nr_foward_edges, 1.0);
		}

		bgraph_t bgraph(edge_array, edge_array + nr_foward_edges,
						weights, g->nr_nodes);
		std::vector<bnode_t> p(boost::num_vertices(bgraph));
		std::vector<double> d(boost::num_vertices(bgraph));

		dist = std::numeric_limits<double>::max();
		for(int i = 0; i < g->nr_nodes; ++i)
		{
			int nr_forward_in = 0;
			for(int j = 0; j < g->nodes[i].nr_in; ++j)
			{
				if(g->edges[g->nodes[i].in[j]].is_backedge)
					continue;
				nr_forward_in++;
			}
			if(nr_forward_in != 0)
				continue;
			bnode_t bsource = boost::vertex(i, bgraph);
			boost::dijkstra_shortest_paths(bgraph, bsource,
							boost::predecessor_map(&p[0]).distance_map(&d[0]));
			double adist = d[target.node];
			if(adist < 0.0)
				adist = std::numeric_limits<double>::max();
			dist = std::min(dist, adist);
		}

		delete edge_array;
		delete weights;
	}
	pthread_mutex_unlock(&g->lock);

out:
	return dist;
}

///////////////////////////////////////////////////
//            Node Ownership Routines            //
///////////////////////////////////////////////////

static int __pgm_claim_node(struct pgm_graph* g, struct pgm_node* n)
{
	int ret = -1;
	int was_error = 0;

	// We open inbound edges first because FIFOs can deadlock otherwise.
	// Open inbound.
	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		struct pgm_node* p = &g->nodes[e->producer];
		ret = e->ops->open_consumer(g, p, n, e);
		if(ret != 0)
			was_error = 1;
	}
	// Open outbound.
	for(int i = 0; i < n->nr_out; ++i)
	{
		struct pgm_edge* e = &g->edges[n->out[i]];
		struct pgm_node* c = &g->nodes[e->consumer];
		ret = e->ops->open_producer(g, n, c, e);
		if(ret != 0)
			was_error = 1;
	}

	ret = (was_error) ? -1 : 0;
	return ret;
}

int pgm_claim_node1(node_t node){
	return pgm_claim_node2(node, 0);
}

int pgm_claim_node2(node_t node, pid_t tid)
{
	int ret = -1;
	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];

	pthread_mutex_lock(&g->lock);
	{
		if(node.node < 0 || node.node >= g->nr_nodes)
		{
			pthread_mutex_unlock(&g->lock);
			goto out;
		}
		n = &g->nodes[node.node];
		if(n->owner != UNCLAIMED_NODE)
		{
			pthread_mutex_unlock(&g->lock);
			goto out;
		}

		n->owner = (tid == 0) ? pgm_gettid() : tid;
	}
	pthread_mutex_unlock(&g->lock);

	ret = __pgm_claim_node(g, n);

out:
	return ret;
}

int pgm_claim_any_node2(graph_t graph, node_t* node){
	return pgm_claim_any_node3(graph, node, 0);
}

int pgm_claim_any_node3(graph_t graph, node_t* node, pid_t tid)
{
	int ret = -1;
	int node_id;
	struct pgm_graph* g;
	struct pgm_node* n = 0;

	if(!is_valid_graph(graph))
		goto out;
	if(!node)
		goto out;

	g = &gGraphs[graph];

	pthread_mutex_lock(&g->lock);
	{
		for(int i = 0; i < g->nr_nodes; ++i)
		{
			if(g->nodes[i].owner == UNCLAIMED_NODE)
			{
				node_id = i;
				n = &g->nodes[i];
				n->owner = (tid == 0) ? pgm_gettid() : tid;
				break;
			}
		}
	}
	pthread_mutex_unlock(&g->lock);

	if(!n)
		goto out;

	ret = __pgm_claim_node(g, n);
	if(ret == 0)
	{
		node->graph = graph;
		node->node = node_id;
	}

out:
	return ret;
}

int pgm_release_node1(node_t node){
	return pgm_release_node2(node, 0);
}

int pgm_release_node2(node_t node, pid_t tid)
{
	int ret = -1;
	int was_error = 0;
	struct pgm_graph* g;
	struct pgm_node* n;

	if(!is_valid_graph(node.graph))
		goto out;

	g = &gGraphs[node.graph];
	n = &g->nodes[node.node];

	if(tid == 0)
	{
		tid = pgm_gettid();
	}

	pthread_mutex_lock(&g->lock);

	if(node.node < 0 || node.node >= g->nr_nodes || n->owner != tid || n->owner == UNCLAIMED_NODE)
		goto out_unlock;

	// Close the FIFOs in the reverse order they were opened (w.r.t. in vs out)
	// Close outbound.
	for(int i = 0; i < n->nr_out; ++i)
	{
		struct pgm_edge* e = &g->edges[n->out[i]];
		ret = e->ops->close_producer(e);
		if(ret != 0)
			was_error = 1;
	}
	// Close inbound.
	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		ret = e->ops->close_consumer(e);
		if(ret != 0)
			was_error = 1;
	}

	n->owner = UNCLAIMED_NODE;

	if(was_error)
		ret = -1;
	else
		ret = 0;

out_unlock:
	pthread_mutex_unlock(&g->lock);

out:
	return ret;
}


///////////////////////////////////////////////////
//          Token Transmission Routines          //
///////////////////////////////////////////////////

typedef unsigned char pgm_command_t;

enum eWaitStatus
{
	WaitSuccess = 0,
	WaitTimeout,
	WaitExhaustedAndTerminate,
	WaitError
};

static int pgm_nr_ready_edges(struct pgm_graph* g, struct pgm_node* n)
{
	int nr_ready = 0;
	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		if(is_signal_driven(e) &&
		   ((e->nr_pending >= e->attr.nr_threshold) || (e->nr_skips > 0)))
		{
			++nr_ready;
		}
	}
	return nr_ready;
}

static void pgm_nr_ready_edges(struct pgm_graph* g, struct pgm_node* n, int& nr_ready, int& nr_ready_normal)
{
	nr_ready = 0;
	nr_ready_normal = 0;
	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		if(is_signal_driven(e) &&
		   ((e->nr_pending >= e->attr.nr_threshold) || (e->nr_skips > 0)))
		{
			++nr_ready;
			if(!e->is_backedge)
			{
				++nr_ready_normal;
			}
		}
	}
}

static eWaitStatus pgm_wait_for_tokens(struct pgm_graph* g, struct pgm_node* n)
{
	int nr_ready, nr_ready_normal;
	unsigned long flags;
	eWaitStatus wait_status = WaitSuccess;

	// quick-path
	pgm_nr_ready_edges(g, n, nr_ready, nr_ready_normal);
	if(nr_ready == n->nr_in_signaled)
		goto out;

	// Have we been signaled to exit, and
	// and we're out of tokens to consume?
	if(nr_ready_normal == 0 &&
	   n->nr_terminate_signals != 0 &&
	   n->nr_terminate_signals == (n->nr_in_signaled - n->nr_in_signaled_backedges))
	{
		wait_status = WaitExhaustedAndTerminate;
		goto out;
	}

	// we have to wait
	pgm_lock(&n->lock, flags);
	do
	{
		// recheck the condition
		pgm_nr_ready_edges(g, n, nr_ready, nr_ready_normal);
		if(nr_ready == n->nr_in_signaled)
			break;
		if(nr_ready_normal == 0 &&
		   n->nr_terminate_signals != 0 &&
		   n->nr_terminate_signals == (n->nr_in_signaled - n->nr_in_signaled_backedges))
		{
			wait_status = WaitExhaustedAndTerminate;
			break;
		}
		// condition still does not hold -- wait for a signal
		pgm_cv_wait(&n->wait, &n->lock, flags);
	}while(1);
	pgm_unlock(&n->lock, flags);

out:
	return wait_status;
}

static void pgm_consume_skips(struct pgm_graph* g, struct pgm_node* n)
{
	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		if(e->nr_skips > 0)
		{
			e->nr_skips--;
		}
	}
}

static void pgm_consume_tokens(struct pgm_graph* g, struct pgm_node* n)
{
	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		if(is_signal_driven(e) && !(e->nr_skips))
		{
			__sync_fetch_and_sub(&e->nr_pending, e->attr.nr_consume);
		}
	}
}

static bool pgm_send_tokens(struct pgm_edge* e)
{
	size_t old_nr_tokens = __sync_fetch_and_add(&e->nr_pending, e->attr.nr_produce);

	if(old_nr_tokens < e->attr.nr_threshold &&
	   old_nr_tokens + e->attr.nr_produce >= e->attr.nr_threshold)
	{
		// we fulfilled the requirements on this edge.
		// we might need to signal the consumer.
		return true;
	}
	return false;
}

#if (PGM_MAX_IN_DEGREE > 32 && PGM_MAX_IN_DEGREE <= 64)
typedef uint64_t pgm_fd_mask_t;
#elif (PGM_MAX_IN_DEGREE > 0 && PGM_MAX_IN_DEGREE <= 32)
typedef uint32_t pgm_fd_mask_t;
#else
typedef uint32_t pgm_fd_mask_t;
#endif

static const unsigned char PGM_NORMAL = 0x01;

static int pgm_send_std_data(struct pgm_edge* e, pgm_command_t tag)
{
	// only the tag is sent if this is a terminate message

	int ret = -1;
	ssize_t bytes;
	size_t sz = (tag & PGM_TERMINATE) ? sizeof(tag) : e->attr.nr_produce + sizeof(tag);
	pgm_command_t* tag_ptr = (pgm_command_t*)pgm_get_user_ptr(e->buf_out) - 1;
	char* buf = (char*)tag_ptr;

	// tag the message
	*tag_ptr = tag;

	while(1)
	{
		bytes = e->ops->write(e, buf, sz);
		if(bytes > 0)
		{
			if((size_t)bytes == sz)
			{
				ret = 0;
				break;
			}
			else
			{
				// we still have more data to send
				buf += bytes;
				sz -= bytes;
			}
		}
		else if(bytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
		{
			// just keep looping
		}
		else
		{
			F("Failed to send data on edge %s\n", e->name);
			ret = -1;
			break;
		}
	}
	return ret;
}

static int pgm_send_ring_data(struct pgm_edge* e, pgm_command_t tag)
{
	if(!(tag & PGM_TERMINATE))
		e->ops->write(e, pgm_get_user_ptr(e->buf_out), e->attr.nr_produce);
	else
		e->ring_cmd = tag;
	return 0;
}

static int pgm_send_data(struct pgm_edge* e, pgm_command_t tag = PGM_NORMAL)
{
	if(!(e->attr.type & __PGM_EDGE_RING))
		return pgm_send_std_data(e, tag);
	else
		return pgm_send_ring_data(e, tag);
}

static eWaitStatus pgm_wait_for_data(pgm_fd_mask_t* to_wait,
				struct pgm_graph* g, struct pgm_node* n)
{
	eWaitStatus wait_status = WaitSuccess;
	fd_set set;
	pgm_fd_mask_t b;
	int sum, i, scanned;

	int num_looped = 0;

	while(*to_wait)
	{
		FD_ZERO(&set);
		sum = 0;

		// build the set
		for(i = 0, b = 1; i < n->nr_in; ++i, b <<= 1)
		{
			if(*to_wait & b)
			{
				FD_SET(g->edges[n->in[i]].fd_in, &set);
				sum += g->edges[n->in[i]].fd_in;
			}
		}

		int nr_ready = select(sum + 1, &set, 0, 0, 0);
		if(nr_ready == 0)
		{
			wait_status = WaitTimeout;
			break;
		}
		else if(nr_ready == -1)
		{
			wait_status = WaitError;
		}

		scanned = 0;
		for(i = 0, b = 1; i < n->nr_in && scanned < nr_ready; ++i, b <<= 1)
		{
			struct pgm_edge* e = &g->edges[n->in[i]];
			if(is_data_passing(e) && FD_ISSET(e->fd_in, &set))
			{
				*to_wait = *to_wait & ~b;
				++scanned;
			}
		}
		++num_looped;
	}

	return wait_status;
}

static eWaitStatus pgm_recv_data(struct pgm_graph* g, struct pgm_node* n)
{
	// TODO: Function must be refactored to remove the heavy abuse of goto.

	eWaitStatus wait_status = WaitSuccess;
	pgm_fd_mask_t to_wait;
	pgm_fd_mask_t to_skip = 0;

	// Each element points to where data needs to be copied.
	// Reads for each edge do not always read all the needed
	// data at once, so we use this array to track the progress
	// of the read for each edge.
	char* dest_ptrs[PGM_MAX_IN_DEGREE];

	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		if(!is_data_passing(e))
		{
			continue;
		}
		if(e->nr_skips > 0)
		{
			to_skip |= ((pgm_fd_mask_t)0x1)<<i;
			continue;
		}
		// initialize to the start of the input edge buffer
		dest_ptrs[i] = (char*)pgm_get_user_ptr(e->buf_in);
	}

	// create a bitmask for each edge to wait upon...
	to_wait = ~((pgm_fd_mask_t)0) >> (sizeof(to_wait)*8 - n->nr_in);

	// (don't read anything if we're skipping all the edges)
	if(to_wait == to_skip)
		goto out;

	// ...but mask out the signal-driven edges and edges we're skipping
	to_wait &= ~(n->signal_edge_mask | to_skip);


wait_for_data: // jump here if we would block on read
	while(to_wait)
	{
		wait_status = pgm_wait_for_data(&to_wait, g, n);
		switch(wait_status)
		{
			case WaitTimeout:
				continue;
			case WaitSuccess:
				break;
			case WaitError:
				   F("select() error for node %s/%s.\n", g->name, n->name);
				goto out;
			default:
				assert(!to_wait);  // unkown error...
				break;
		}
	}

	if(wait_status != WaitSuccess)
		return wait_status;

	// all edges are ready for reading, according to select()
	for(int i = 0; i < n->nr_in; ++i)
	{
		struct pgm_edge* e = &g->edges[n->in[i]];
		ssize_t bytes_read;
		ssize_t remaining;

		// skip over non-data-passing edges
		if(!is_data_passing(e))
			continue;
		if(e->nr_skips > 0)
			continue;

		if(e->attr.type & __PGM_EDGE_RING)
		{
			/* short-cut for the simple ring buffer IPC */
			if(!((e->ring_cmd & PGM_TERMINATE) && is_ring_empty(&e->ringbuf)))
				e->ops->read(e, dest_ptrs[i], e->attr.nr_consume);
			else
				n->nr_terminate_msgs++;
			continue;
		}

read_more: // jump to here if we need to read more bytes into our buffer

		remaining = e->attr.nr_consume - (dest_ptrs[i] - (char*)pgm_get_user_ptr(e->buf_in));
		assert(remaining > 0);
		if((size_t)remaining == e->attr.nr_consume && e->next_tag == 0)
		{
			// We haven't read any data for this edge yet, and the next byte is the start of
			// a message header tag. We'll handle this special case in one operation in
			// order to support POSIX message queues, where we can't read the tag and data
			// piece-wise. NOTE: produce/consume amount must be equal for MQs, so we
			// can always read the tag into space reserved before the buffer.

			// Read the tag directly into the space reserved just before the start of the
			// consumer's buffer. We cannot read more than the amount produced by the
			// producer before we hit the next tag.
			size_t chunk_size = (e->attr.nr_consume <= e->attr.nr_produce) ?
					e->attr.nr_consume : e->attr.nr_produce;
			pgm_command_t* tag_ptr = ((pgm_command_t*)dest_ptrs[i])-1;
			bytes_read = e->ops->read(e, tag_ptr, chunk_size + sizeof(pgm_command_t));
			if(bytes_read > 0)
			{
				bytes_read -= sizeof(pgm_command_t); // don't inc. tag in read count

				e->next_tag = e->attr.nr_produce - bytes_read;

				// check for termination
				if(*tag_ptr & PGM_TERMINATE)
				{
					n->nr_terminate_msgs++;
					continue; // we're done with this edge
				}
				if(!(*tag_ptr & PGM_NORMAL))
				{
					E("Malformed data stream detected on edge %s\n", e->name);
					wait_status = WaitError;
					goto out;
				}

				// read more if we haven't read all that we need to consume
				if((size_t)bytes_read != e->attr.nr_consume)
				{
					dest_ptrs[i] += bytes_read;
					goto read_more;
				}
			}
		}
		else if(e->next_tag != 0)
		{
			// We have partially read data into our buffer and
			// we need to read more. However, don't include the
			// producers tag in our next read--only read up
			// to the next tag.
			size_t chunk_size = ((size_t)remaining <= e->next_tag) ? remaining : e->next_tag;
			bytes_read = e->ops->read(e, dest_ptrs[i], chunk_size);
			if(bytes_read > 0)
			{
				e->next_tag -= bytes_read;
				if(remaining != bytes_read)
				{
					// we need to read more data, so update dest pointer
					// and reissue the read for more data.
					dest_ptrs[i] += bytes_read;
					goto read_more;
				}
			}
		}
		else
		{
			// Next byte is the start of a message header, and we've already
			// read some data for this edge. We must read this message header
			// into out-of-band memory since we don't have a safe place for it
			// in the consumer's buffer.
			pgm_command_t tag;
			bytes_read = e->ops->read(e, &tag, sizeof(tag));
			if(bytes_read > 0)
			{
				e->next_tag = e->attr.nr_produce;

				if(tag & PGM_TERMINATE)
				{
					n->nr_terminate_msgs++;
					continue; // we're done with this edge.
				}
				if(!(tag & PGM_NORMAL))
				{
					E("Malformed data stream detected on edge %s\n", e->name);
					wait_status = WaitError;
					goto out;
				}
				goto read_more;
			}
		}

		if(bytes_read == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
		{
			// We need to block again on select(). Block on this edge,
			// and all those we have yet to handle.

			// recompute a mask for this edge and all after i.
			to_wait = ~((pgm_fd_mask_t)0) >> (sizeof(to_wait)*8 - (i + 1));
			// ...but mask out signal-driven edges and edge's we're skipping
			to_wait &= ~(n->signal_edge_mask | to_skip);
			// ...but still make sure that we wait for this edge,
			// even if it's a singalled one (we can't be reading an edge
			// that we're skipping since we would have skipped it and
			// there would have been no failure).
			to_wait |= ((pgm_fd_mask_t)1)<<i;

			goto wait_for_data;
		}
		else if(bytes_read == -1)
		{
			F("read() error for edge %s/%s of node %s/%s.\n",
				g->name, e->name, g->name, n->name);

			wait_status = WaitError;
			goto out;
		}
	}

out:
	// signal terminte if everyone has checked in
	if(n->nr_terminate_msgs &&
	   (n->nr_terminate_msgs == (n->nr_in_data - n->nr_in_data_backedges)))
		wait_status = WaitExhaustedAndTerminate;

	return wait_status;
}

int pgm_wait(node_t node)
{
	int ret = -1;
	struct pgm_graph* g = &gGraphs[node.graph];
	struct pgm_node* n = &g->nodes[node.node];

	eWaitStatus token_status = WaitExhaustedAndTerminate;
	eWaitStatus data_status = WaitExhaustedAndTerminate;

	// no locking or error checking for the sake of speed.
	// we assume initialization is done. use higher-level constructs, such
	// as barriers, to ensure clean bring-up and shutdown.

	// wait to be signaled before attempting to read
	if(n->nr_in_signaled)
	{
		token_status = pgm_wait_for_tokens(g, n);
	}
	if(n->nr_in_data)
	{
		data_status = pgm_recv_data(g, n);
	}

	if(token_status == WaitExhaustedAndTerminate &&
	   data_status == WaitExhaustedAndTerminate)
	{
		ret = PGM_TERMINATE;
	}
	else
	{
		ret = (token_status == WaitTimeout || token_status == WaitError ||
		       data_status  == WaitTimeout || data_status  == WaitError) ?
				-1 : 0;
	}

	if(n->nr_in_signaled && ret != PGM_TERMINATE)
		pgm_consume_tokens(g, n);  // consume the token counters

	pgm_consume_skips(g, n); // decrement skip counts

	if(ret == PGM_TERMINATE)
		pgm_terminate(node);

	return ret;
}

static int pgm_produce(node_t node, pgm_command_t command = PGM_NORMAL)
{
	int ret = -1, was_error = 0;
	struct pgm_graph* g = &gGraphs[node.graph];
	struct pgm_node* n = &g->nodes[node.node];
	struct pgm_edge* e;

	struct pgm_node* to_wake[PGM_MAX_OUT_DEGREE];
	int nr_to_wake = 0;

	// no locking or error checking for the sake of speed.
	// we assume initialization is done. use higher-level constructs, such
	// as barriers, to ensure clean bring-up and shutdown.

	for(int i = 0; i < n->nr_out; ++i)
	{
		e = &g->edges[n->out[i]];

		// terminate messages are not sent over backedges
		if((command & PGM_TERMINATE) && e->is_backedge)
			continue;

		if(is_data_passing(e))
		{
			ret = pgm_send_data(e, command);
			if(ret)
				was_error = 1;
		}
		if(is_signal_driven(e))
		{
			if(!(command & PGM_TERMINATE))
			{
				if(pgm_send_tokens(e))
					to_wake[nr_to_wake++] = &g->nodes[e->consumer];
			}
			else
			{
				__sync_fetch_and_add(&g->nodes[e->consumer].nr_terminate_signals, 1);
				to_wake[nr_to_wake++] = &g->nodes[e->consumer];
			}
		}
	}

	for(int i = 0; i < nr_to_wake; ++i)
	{
		struct pgm_node* c = to_wake[i];
		unsigned long flags;
		pgm_lock(&c->lock, flags);
		if((command & PGM_TERMINATE) || pgm_nr_ready_edges(g, c) == c->nr_in_signaled)
			pgm_cv_signal(&c->wait);
		pgm_unlock(&c->lock, flags);
	}

	ret = (was_error) ? -1 : 0;

	return ret;
}

int pgm_complete(node_t node)
{
	return pgm_produce(node);
}

int pgm_terminate(node_t node)
{
	return pgm_produce(node, PGM_TERMINATE);
}

static const char* edgeTypeStr(const struct pgm_edge* e)
{
	if(e->ops == &pgm_ring_edge_ops)
		return "ring";
	if(e->ops == &pgm_fifo_edge_ops)
		return "fifo";
	if(e->ops == &pgm_mq_edge_ops)
		return "mq";
	if(e->ops == &pgm_sock_stream_edge_ops)
		return "stream";
	if(e->ops == &pgm_cv_edge_ops)
		return "cv";
	return "unknown";
}

static void filter_ctrlchars(char* str, size_t len)
{
	for(size_t i = 0; i < len && str[i] != '\0'; ++i)
	{
		if(iscntrl(str[i]))
		{
			str[i] = '\0';
			break;
		}
	}
}

int pgm_print_graph(graph_t graph, FILE* outs)
{
	int ret = -1;
	struct pgm_graph* g;

	char namebuf[PGM_NODE_NAME_LEN];
	char thresh[80];

	if(!is_valid_graph(graph))
		goto out;

	g = &gGraphs[graph];

	pthread_mutex_lock(&g->lock);

	strncpy(namebuf, g->name, PGM_NODE_NAME_LEN);
	filter_ctrlchars(namebuf, PGM_NODE_NAME_LEN);

	fprintf(outs,
		"digraph G {\n"
		"\trankdir=TB;\n"
		"\tsize=\"11,8.5\";\n"
		"\tlabel=\"%s\";\n",
		namebuf);

	for(int i = 0; i < g->nr_nodes; ++i)
	{
		const struct pgm_node* n = &g->nodes[i];

		int degreeIn = 0;
		int degreeOut = 0;

		for(int j = 0; j < n->nr_in; ++j)
		{
			if(!g->edges[n->in[j]].is_backedge)
			{
				++degreeIn;
			}
		}

		for(int j = 0; j < n->nr_out; ++j)
		{
			if(!g->edges[n->out[j]].is_backedge)
			{
				++degreeOut;
			}
		}

		bool isSrc = (degreeIn == 0);
		bool isSink = (degreeOut == 0);

		strncpy(namebuf, n->name, PGM_NODE_NAME_LEN);
		filter_ctrlchars(namebuf, PGM_NODE_NAME_LEN);

		fprintf(outs,
			"\t%d [shape=%s, style=%s, color=%s, label=\"%s\"];\n",
			i,
			(isSrc || isSink) ? "doublecircle" : "circle",
			(isSrc || isSink) ? "filled" : "\"\"",
			(isSrc) ? "\"#56A0D3\"" : (isSink) ? "\"#E04050D3\"" : "\"\"",
			namebuf);
	}

	for(int i = 0; i < g->nr_edges; ++i)
	{
		const struct pgm_edge* e = &g->edges[i];

		strncpy(namebuf, e->name, PGM_NODE_NAME_LEN);
		filter_ctrlchars(namebuf, PGM_NODE_NAME_LEN);

		if(!e->is_backedge)
		{
			snprintf(thresh, sizeof(thresh), "(%lu)", e->attr.nr_threshold);
		}
		else
		{
			snprintf(thresh, sizeof(thresh), "(%lu, %lu)", e->attr.nr_threshold, e->nr_skips);
		}

		fprintf(outs,
			"\t%d -> %d [style=%s, color=%s, label=\"%s%s_%s\", headlabel=\"%lu %s\", taillabel=\"%lu\"]\n",
			e->producer,
			e->consumer,
			(!e->is_backedge) ? "solid" : "dashed",
			(is_data_passing(e)) ? "blue" : "dimgray",
			(is_signal_driven(e) && e->ops != &pgm_cv_edge_ops) ? "fast_" : "",
			edgeTypeStr(e),
			namebuf,
			e->attr.nr_consume,
			thresh,
			e->attr.nr_produce);
	}

	fprintf(outs, "}\n");

	pthread_mutex_unlock(&g->lock);

	ret = 0;

out:
	return ret;
}
