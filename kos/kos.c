#include <unistd.h>     /* Symbolic Constants */
#include <errno.h>      /* Errors */
#include <stdio.h>      /* Input/Output */
#include <stdlib.h>     /* General Utilities */
#include <string.h>     /* String handling */
#include <pthread.h>    /* Thread function */
#include <semaphore.h>  /* Semaphore */

#include <kos_client.h>
#include "kos_client.h"
#include "list.h"
#include "buffer.h"
#include "delay.h"	
#include "hash.h"

#define HT_SIZE 30 //Garantir que e um numero maior que NUM_CLIENT_THREADS
#define MAX_CHARS 20

#define DEBUG_PRINT_ENABLED 1  // uncomment to enable DEBUG statements
#if DEBUG_PRINT_ENABLED
#define DEBUG printf
#else
#define DEBUG(format, args...) ((void)0)
#endif

void init_reqBuffer();
void consumer_server( void *ptr );

int nshards;
int req_buff_size;
FILE *file;

//Server threads
pthread_t* server_threads;


/* semaphores are global. Some of them are used as a mutex */
sem_t mutex;
sem_t fillCount;  //used to control the acess of the buffer
sem_t emptyCount; //used to control the acess of the buffer
sem_t put_mutex;
sem_t get_mutex;
sem_t rem_mutex;
sem_t getAll_mutex;
sem_t write_mutex;

//Struct for each partition of KOS (shard)
typedef struct kos_shard
{
    list_t lists[HT_SIZE];
} kos_shard;


//Pointer to a shard partition
kos_shard* k_shard;

//Creates a new key-value pair
KV_t* new_pair(char* key, char* value);

//Inserts a pair in KOS
void kos_insert(kos_shard* k_shard, KV_t* pair, int clientId, int shardId);

//Initializates hash lists
void kos_init_lists(kos_shard* k_shard);

//Pointer to be used in server threads argument
int* ids;

//Variables used to control the acess of the buffer
int consIndex = 0;
int freeIndex = 0;

//Pointer to the buffer
buffer_t** req_buffer;

/*Initialize the buffer vector and the KOS vector containing 'num_shards' partitions
This function returns:
   *   0, if the initialization was successful; 
   *  -1 if the initialization failed;   */
int kos_init(int num_server_threads, int buf_size, int num_shards){
	int s, i;
        k_shard = (kos_shard*)malloc(sizeof(kos_shard) * num_shards);
	kos_init_lists(k_shard);
	server_threads =(pthread_t*)malloc(sizeof(pthread_t)* num_server_threads);
        ids = (int*)malloc(sizeof(int)*num_server_threads);
	req_buff_size = buf_size;
	init_reqBuffer(buf_size);
	sem_init(&mutex, 0, 1);      /* initialize mutex to 1 - binary semaphore */
	sem_init(&fillCount, 0, 0);      /* initialize mutex to 0 - binary semaphore */
        sem_init(&emptyCount, 0, buf_size);      /* initialize mutex to buffSize - binary semaphore */
        sem_init(&put_mutex, 0, 1);     
        sem_init(&get_mutex, 0, 1); 
        sem_init(&rem_mutex, 0, 1);  
	sem_init(&getAll_mutex, 0, 1);
        sem_init(&write_mutex, 0, 1);  
	nshards = num_shards;       

	for (i=0; i<num_server_threads; i++) {	
		ids[i]=i;		
		
		if ( (s=pthread_create(&server_threads[i], NULL, &consumer_server, &(ids[i])) ) ) {
			printf("pthread_create failed with code %d!\n",s);
			return -1;
		}
	}

	return 0;
}

//Search for the a key in the KOS, returning the value of that key if it exits, NULL otherwise
char* search_key(kos_shard* k_shard, buffer_t* req){

	int i, j;
	int found = 0;
	lst_iitem_t* temp;
	if(k_shard){
		temp = k_shard[req->shardId].lists[req->clientId].first;
			while(temp){
				if (!(strcmp(req->kv.key, temp->value->key))){
					return temp->value->value; 
				} else {temp = temp->next;}
			}
  	}
	//printf("Chave nao encontrada\n");
	return NULL;
}

//Replaces the older key value for a new one
int replace_kvalue(kos_shard* k_shard, int clientId, int shardId, char* key, char* value){

	lst_iitem_t* temp;
	temp = k_shard[shardId].lists[clientId].first;

	while(temp){
		if(!(strcmp(key, temp->value->key))){
			memcpy(temp->value->value, value, 20);
			return 0;
		} else {temp = temp->next;}
 	}
}



//Alocates the memory for a new key-value pair
KV_t* new_pair(char* key, char* value)
{
   	KV_t* pair = (KV_t*)malloc(sizeof(KV_t));
   	memcpy(pair->key, key, MAX_CHARS);  
	memcpy(pair->value, value, MAX_CHARS); 
	return pair;
}



//This function filters the request made a client and process it
void process_request(buffer_t* req){
	char* ret;
	char* retVal;
	char* retPut;
	int type = req->type;
	int res;
	lst_iitem_t* tmp;
	int size;
	lst_iitem_t* temp;
	KV_t* allKeys;
	int i;
	int returnNull = 0;
	switch(type){

		case 0:
			sem_wait(&get_mutex);
			tmp = k_shard[req->shardId].lists[req->clientId].first;
			while (1){
				if(!tmp){
				strcpy(req->kv.value, "NULL");
				break;
				}
				if (!(strcmp(req->kv.key, tmp->value->key))){
					//printf("KOS_GET: Encontrou chave com valor: %s\n", tmp->value->value);
					strcpy(req->kv.value, tmp->value->value);
					break;
				} else {
					if (!tmp->next) {
						//printf("KOS_GET: Key nao encontrada\n");
						strcpy(req->kv.value, "NULL");
						break;
					} else {tmp = tmp->next;}
				}
        	     	}

			sem_post(&get_mutex);
			break;


		case 1: 
			sem_wait(&put_mutex);
			ret = search_key(k_shard, req);	
			if (!ret) {
				kos_insert(k_shard, new_pair(req->kv.key, req->kv.value), req->clientId, req->shardId);	
                                strcpy(req->kv.value, "NULL");
				returnNull = 1;
			} else {
				retPut = (char*)malloc(sizeof(char)*20);
				memcpy(retPut, ret, 20);
				replace_kvalue(k_shard, req->clientId, req->shardId, req->kv.key, req->kv.value);
				strcpy(req->kv.value, retPut);
	           	}
			sem_post(&put_mutex);
			break;


		case 2:

			sem_wait(&rem_mutex);
			retVal = search_key(k_shard, req);
			if(!retVal){
				strcpy(req->kv.value, "NULL");		
			}
			else {
                	strcpy(req->kv.value, retVal);
			res = lst_remove(&k_shard[req->shardId].lists[req->clientId], req->kv.key);
			}
			sem_post(&rem_mutex);
			break;


		case 3:
			sem_wait(&getAll_mutex);
			size = 0;
			if(!k_shard[req->shardId].lists[req->clientId].first){
				size = 0;
				req->allK = NULL;

				break;
				} else {
					temp = k_shard[req->shardId].lists[req->clientId].first;

					while(temp){
						if(temp){
							size += 1;
						 }
					temp = temp->next;
					}}
				req->dim = size;
				allKeys = (KV_t*)malloc(sizeof(KV_t) * size);
				temp = k_shard[req->shardId].lists[req->clientId].first;

				//Fills KV_t* list to be returned
				if(!temp){
					req->allK = NULL;
				}
				else {
				for(i= 0; i< size; i++){
					if(temp){
						strcpy(allKeys[i].key,  temp->value->key);
						strcpy(allKeys[i].value,  temp->value->value);
				 	}
				temp = temp->next;
				}
		
		req->allK = (KV_t*)malloc(sizeof(KV_t) * size);
		req->allK = allKeys;
		}
		sem_post(&getAll_mutex);
		break;
	}
}

//Initializates the buffer to be used as client-server comunication
void init_reqBuffer(int buf_size){

	req_buffer = (buffer_t**)malloc(sizeof(buffer_t*) * buf_size);
	memset(req_buffer, NULL, sizeof(buffer_t*) * buf_size);
}


//Sends a kos_put request to buffer, returning a value after server process it
char* kos_put(int clientId, int shardId, char* key, char* value){
	buffer_t* req = (buffer_t*)malloc(sizeof(buffer_t));
	req->type = 1;
	req->clientId = clientId;
	req->shardId = shardId;
        strncpy(req->kv.key, key, MAX_CHARS);
        strncpy(req->kv.value, value, MAX_CHARS);
	sem_init(&req->sem, 0, 0);
	sem_wait(&emptyCount);
            sem_wait(&mutex);

		// Coloca pedido no buffer
		req_buffer[freeIndex] = req;
		freeIndex = (freeIndex + 1) % req_buff_size;
		//printf("ProdutorPUT: Item introduzido no buffer\n");
	    sem_post(&mutex);
	sem_post(&fillCount);
 
	// Espera resposta do servidor
	sem_wait(&req->sem);

	sem_wait(&write_mutex);
	writeToFile(); // Actualiza o ficheiro com o par introduzido no kos
	sem_post(&write_mutex);

	// Devolve a resposta
	if(strcmp(req->kv.value, "NULL")) {
	printf("KOS_PUT retorna %s\n", req->kv.value);
        return strdup(req->kv.value);
	}
	else {
	printf("KOS_PUT retorna NULL\n");
	return NULL;	
	}
}

//Sends a kos_get request to buffer, returning a value after server process it
char* kos_get(int clientId, int shardId, char* key){

	buffer_t* req = (buffer_t*)malloc(sizeof(buffer_t));
	req->type = 0;
	req->clientId = clientId;
	req->shardId = shardId;
        strncpy(req->kv.key, key, MAX_CHARS);
	sem_init(&req->sem, 0, 0);
	sem_wait(&emptyCount);
            sem_wait(&mutex);
		// Coloca pedido no buffer
		req_buffer[freeIndex] = req;
		freeIndex = (freeIndex + 1) % req_buff_size;
		//printf("ProdutorGet: Item introduzido no buffer\n");
	    sem_post(&mutex);
	sem_post(&fillCount);
 
	// Espera resposta do servidor
	sem_wait(&req->sem);
		
	// Devolve a resposta
	if(strcmp(req->kv.value, "NULL")) {
	printf("KOS_GET retorna %s\n", req->kv.value);
        return strdup(req->kv.value);
	}
	else {
	printf("KOS_GET retorna NULL\n");
	return NULL;	
	}
}

//Sends a kos_remove request to buffer, returning a value after server process it
char* kos_remove(int clientId, int shardId, char* key){

	buffer_t* req = (buffer_t*)malloc(sizeof(buffer_t));
	req->type = 2;
	req->clientId = clientId;
	req->shardId = shardId;
        strncpy(req->kv.key, key, MAX_CHARS);
	sem_init(&req->sem, 0, 0);
	sem_wait(&emptyCount);
            sem_wait(&mutex);
		// Coloca pedido no buffer
		req_buffer[freeIndex] = req;
		freeIndex = (freeIndex + 1) % req_buff_size;
		//printf("ProdutorRemove: Item introduzido no buffer\n");
	    sem_post(&mutex);
	sem_post(&fillCount);
 
	// Espera resposta
	sem_wait(&req->sem);

	sem_wait(&write_mutex);
	writeToFile(); // Actualiza o ficheiro com o par removido do kos
	sem_post(&write_mutex);
		
	// Devolve a resposta
	if(strcmp(req->kv.value, "NULL")) {
	printf("KOS_REMOVE retorna %s\n", req->kv.value);
        return strdup(req->kv.value);
	}
	else {
	printf("KOS_REMOVE retorna NULL\n");
	return NULL;	
	}
	
}


/* returns an array of KV_t containing all the key value pairs stored in the specified shard of KOS (NULL in case the shard is empty). It stores in dim the number of key/value pairs present in the specified shard. If the clientId or shardId are invalid (e.g. too large with respect to the value specified upon inizialitization of KOS), this function assigns -1 to the "dim" parameter.   */
KV_t* kos_getAllKeys(int clientid, int shardId, int* dim){
	buffer_t* req = (buffer_t*)malloc(sizeof(buffer_t));
	req->type = 3;
	req->clientId = clientid;
	req->shardId = shardId;
	//req->dim = &dim;
	sem_init(&req->sem, 0, 0);
	sem_wait(&emptyCount);
            sem_wait(&mutex);
		// Coloca pedido no buffer
		req_buffer[freeIndex] = req;
		freeIndex = (freeIndex + 1) % req_buff_size;
		//printf("ProdutorRemove: Item introduzido no buffer\n");
	    sem_post(&mutex);
	sem_post(&fillCount);
 
	// Espera resposta
	sem_wait(&req->sem);
		
	// Devolve a resposta
	*dim = req->dim;
	return req->allK;
}


// Initialize all shards and their linked-lists
void kos_init_lists(kos_shard* k_shard)
{
   int i, j;
   if(k_shard)
	for(i = 0; i < nshards; i++){ 

		for(j = 0; j < HT_SIZE; j++){
         k_shard[i].lists[j].first = NULL;
		}
	}
}


//Inserts a new key-value pair in the KOS
void kos_insert(kos_shard* k_shard, KV_t* pair, int clientId, int shardId)
{
   lst_insert(&k_shard[shardId].lists[clientId], pair);
}


//List all key-value pairs stored in KOS
void kos_list(kos_shard* k_shard)
{
	int i, j;
	for(i = 0; i < nshards; i++){
		printf("\n--------------------\n");
		printf("Contents of shardId %d\n", i);
		printf("--------------------\n");
		for(j= 0; j < HT_SIZE; j++){
			printf("clientId; %d\n", j);
			lst_print(&k_shard[i].lists[j]);			
		}

	}
	

}

//Prints the contents of a specific list in the hash table
void fprintfShards(list_t *list)
{
   lst_iitem_t* tmp;

   if(list)
   {
      tmp = list->first;

      if(!tmp)
         fprintf(file, " ");

      while(tmp){
	 fprintf(file, "(%s, %s), ", tmp->value->key, tmp->value->value);
         tmp = tmp->next;
      }
   }
}

//Auxiliar function to write the shard content in a file
void writeShard(int shardId){
	int j;
	fprintf(file,"A imprimir shard: %d", shardId); /*writes*/
	for(j= 0; j < HT_SIZE; j++){
			fprintf(file, "\nclientId %d: ", j);
			fprintfShards(&k_shard[shardId].lists[j]);			
		}


}

//Creates a file if it doesn't exist and writes its content
void writeToFile(){

	char filename[20];
	int shardId, i;
	for (i= 0; i< 4; i++){
		sprintf(filename, "fshard%d.txt", i);
		file = fopen(filename,"w"); 
		writeShard(i);
		fclose(file); 

	}

}


//Server thread used to process client requests 
void consumer_server( void *ptr ) {
    buffer_t* req;

    while (1) {
        sem_wait(&fillCount);
	//delay();
            sem_wait(&mutex);
		req = req_buffer[consIndex];
		consIndex = (consIndex + 1) % req_buff_size;
		//printf("Consumidor: retirou pedido do buffer by: %d\n", *((int *) ptr));
            sem_post(&mutex);
        sem_post(&emptyCount);

	// Resolve a resposta
	process_request(req);

        // Liberta cliente
        sem_post(&req->sem);
    }

    pthread_exit(0); /* exit thread */
}
