#include <stdlib.h>
#include <stdio.h>
#include "kos_client.h"
#include <semaphore.h>  /* Semaphore */

//Buffer struct
typedef struct buffer {

	int type; //Type of request: 0- kos_get;  1- kos_put;  2- kos_remove;  3-  kos_getAllKeys
	int clientId;
	int shardId;
	KV_t kv;
	sem_t sem;
	int dim;
	KV_t* allK;

} buffer_t;


//Pointer to the buffer
buffer_t* buffer;

//Number of shard defined in kos_init
int n_shards;

void printfBuffer();

void resetBuffer(int clientId);


