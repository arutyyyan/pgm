// Copyright (c) 2014, Glenn Elliott
// All rights reserved.

/* A program for testing the basic ring-based edge. */

#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>

#include "pgm.h"

int errors = 0;
pthread_barrier_t init_barrier;

__thread char __errstr[80] = {0};

#define CheckError(e) \
do { int __ret = (e); \
if(__ret < 0) { \
	errors++; \
	char* errstr = strerror_r(errno, __errstr, sizeof(errstr)); \
	fprintf(stderr, "%lu: Error %d (%s (%d)) @ %s:%s:%d\n",  \
		pthread_self(), __ret, errstr, errno, __FILE__, __FUNCTION__, __LINE__); \
}}while(0)

int TOTAL_ITERATIONS = 10;

void* thread1(void* _node)
{
	char tabbuf[] = "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";
	int iterations = 1;
	int ret = 0;

	node_t node = *((node_t*)_node);

	CheckError(pgm_claim_node1(node));
	tabbuf[node.node] = '\0';

	int out_degree = pgm_get_degree_out1(node);
	edge_t* out_edges = (edge_t*)calloc(out_degree, sizeof(edge_t));
	int* buf_out;



	buf_out = (int*)pgm_get_edge_buf_p(out_edges[0]);

	int sum = 0;

	pthread_barrier_wait(&init_barrier);

	if(!errors)
	{
		do {

			if(iterations > TOTAL_ITERATIONS)
				ret = PGM_TERMINATE;

			if(ret != PGM_TERMINATE)
			{
				CheckError(ret);
				*buf_out = iterations;

				sum += iterations;

				CheckError(pgm_complete(node));
				iterations++;
			}
			else
			{
				fprintf(stdout, "%s%d terminates: sum: %lu\n", tabbuf, node.node, sum);

				CheckError(pgm_terminate(node));
			}

		} while(ret != PGM_TERMINATE);
	}

	pthread_barrier_wait(&init_barrier);

	CheckError(pgm_release_node1(node));

	free(out_edges);

	pthread_exit(0);
}


void* thread2(void* _node)
{
	char tabbuf[] = "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";
	int iterations = 1;
	int ret = 0;


	node_t node = *((node_t*)_node);

	CheckError(pgm_claim_node1(node));
	tabbuf[node.node] = '\0';

	int out_degree = pgm_get_degree_out1(node);
	int in_degree = pgm_get_degree_in1(node);
	edge_t* out_edges = (edge_t*)calloc(out_degree, sizeof(edge_t));
	edge_t* in_edges = (edge_t*)calloc(in_degree, sizeof(edge_t));

	int* buf_in;
	int* buf_out;

	buf_out = (int*)pgm_get_edge_buf_p(out_edges[0]);
	buf_in = (int*)pgm_get_edge_buf_c(in_edges[0]);

	int sum = 0;
	printf("thread2\n");

	pthread_barrier_wait(&init_barrier);

	if(!errors)
	{

		do {
			ret = pgm_wait(node);

			if(ret != PGM_TERMINATE)
			{
				CheckError(ret);


				sum += *buf_in;
				*buf_out = *buf_in;
				printf("thread 2 %d\n", *buf_in);

				fprintf(stdout, "%s%d fires. read:%d, sum: %d\n", tabbuf, node.node, *buf_in, sum);
				printf("%d\n", sum);

									// slow down the consumer a little bit to induce backlog in token buffer
				if(rand()%5 == 0)
					sched_yield();


				CheckError(pgm_complete(node));
				iterations++;
			}
			else
			{
				fprintf(stdout, "%s%d terminates: sum: %lu\n", tabbuf, node.node, sum);

			}

		} while(ret != PGM_TERMINATE);
	}

	pthread_barrier_wait(&init_barrier);

	CheckError(pgm_release_node1(node));

	free(out_edges);
	free(in_edges);

	pthread_exit(0);
}


void* thread3(void* _node)
{
	char tabbuf[] = "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";
	int iterations = 1;
	int ret = 0;
	printf("thread3\n");

	node_t node = *((node_t*)_node);

	CheckError(pgm_claim_node1(node));
	tabbuf[node.node] = '\0';

	int in_degree = pgm_get_degree_in1(node);
	edge_t* in_edges = (edge_t*)calloc(in_degree, sizeof(edge_t));

	int* buf_in;

	buf_in = (int*)pgm_get_edge_buf_c(in_edges[0]);

	int sum = 0;

	pthread_barrier_wait(&init_barrier);

	if(!errors)
	{

		do {
			ret = pgm_wait(node);

			if(ret != PGM_TERMINATE)
			{
				CheckError(ret);
				sum += *buf_in;

				fprintf(stdout, "%s%d fires. read:%d, sum: %d\n", tabbuf, node.node, *buf_in, sum);
				printf("%d\n", sum);

									// slow down the consumer a little bit to induce backlog in token buffer
				if(rand()%5 == 0)
					sched_yield();


				CheckError(pgm_complete(node));
				iterations++;
			}
			else
			{
				fprintf(stdout, "%s%d terminates: sum: %lu\n", tabbuf, node.node, sum);

			}

		} while(ret != PGM_TERMINATE);
	}

	pthread_barrier_wait(&init_barrier);

	CheckError(pgm_release_node1(node));

	free(in_edges);

	pthread_exit(0);
}



int main(void)
{
	graph_t g;
	node_t  n0, n1, n2;
	edge_t  e0_1, e1_2;

	pthread_t t0, t1, t2;

	edge_attr_t ring_attr;
	memset(&ring_attr, 0, sizeof(ring_attr));
	ring_attr.type = pgm_ring_edge;
	ring_attr.nr_produce = sizeof(int);
	ring_attr.nr_consume = sizeof(int);
	ring_attr.nr_threshold = sizeof(int);
	ring_attr.nmemb = 10;

	CheckError(pgm_init_process_local());
	CheckError(pgm_init_graph(&g, "demo"));

	CheckError(pgm_init_node(&n0, g, "n0"));
	CheckError(pgm_init_node(&n1, g, "n1"));
	CheckError(pgm_init_node(&n2, g, "n2"));

	CheckError(pgm_init_edge5(&e0_1, n0, n1, "e0_1", &ring_attr));
	CheckError(pgm_init_edge5(&e1_2, n1, n2, "e1_2", &ring_attr));

	pthread_barrier_init(&init_barrier, 0, 3);
	pthread_create(&t0, 0, thread1, &n0);
	pthread_create(&t1, 0, thread2, &n1);
	pthread_create(&t2, 0, thread3, &n2);

	pthread_join(t0, 0);
	pthread_join(t1, 0);
	pthread_join(t2, 0);
	//usleep(5000);

	CheckError(pgm_destroy_graph(g));

	CheckError(pgm_destroy());

	return 0;
}
