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

#define NUM_EL 100
#define NUM_SHARDS 10
#define NUM_CLIENT_THREADS 20
#define NUM_SERVER_THREADS 4
#define KEY_SIZE 20

#define HT_SIZE 30
#define REQ_BUFF_SIZE 20
#define MAX_CHARS 20

#define DEBUG_PRINT_ENABLED 1  // uncomment to enable DEBUG statements
#if DEBUG_PRINT_ENABLED
#define DEBUG printf
#else
#define DEBUG(format, args...) ((void)0)
#endif

void init_reqBuffer();
void consumer_server( void *ptr );
void producer_client( void *ptr );
int nshards;

//Serevr threads
pthread_t* server_threads;
pthread_t server_thread2;

/* semaphores are global so they can be accessed thread routine
In this case, the semaphore is used as a mutex */
sem_t mutex;
sem_t fillCount;
sem_t emptyCount;
sem_t put_mutex;
sem_t get_mutex;
sem_t rem_mutex;
sem_t getAll_mutex;

//Struct for each partition of KOS (shard)
typedef struct kos_shard
{
    list_t lists[HT_SIZE];
} kos_shard;


//Pointer to a shard partition
kos_shard* k_shard;


KV_t* new_pair(char* key, char* value);


void kos_insert(kos_shard* k_shard, KV_t* pair, int clientId, int shardId);
void kos_init_lists(kos_shard* k_shard);

int* ids;

//Initialize the buffer vector and the KOS vector containing 'num_shards' partitions
int kos_init(int num_server_threads, int buf_size, int num_shards){
	int s, i;
        k_shard = (kos_shard*)malloc(sizeof(kos_shard) * num_shards);
	kos_init_lists(k_shard);
	server_threads =(pthread_t*)malloc(sizeof(pthread_t)* num_server_threads);
        ids = (int*)malloc(sizeof(int)*num_server_threads);
	init_reqBuffer();
	sem_init(&mutex, 0, 1);      /* initialize mutex to 1 - binary semaphore */
	sem_init(&fillCount, 0, 0);      /* initialize mutex to 0 - binary semaphore */
        sem_init(&emptyCount, 0, REQ_BUFF_SIZE);      /* initialize mutex to buffSize - binary semaphore */
        sem_init(&put_mutex, 0, 1);     
        sem_init(&get_mutex, 0, 1); 
        sem_init(&rem_mutex, 0, 1);  
	sem_init(&getAll_mutex, 0, 1);
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



//Alocates the memory for the
KV_t* new_pair(char* key, char* value)
{
   	KV_t* pair = (KV_t*)malloc(sizeof(KV_t));
   	memcpy(pair->key, key, MAX_CHARS);  //Testar troca de 20 para sizeof(key)
	memcpy(pair->value, value, MAX_CHARS); 
	return pair;
}




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
				//printf("Entrou no !ret\n");
				kos_insert(k_shard, new_pair(req->kv.key, req->kv.value), req->clientId, req->shardId);	
                                strcpy(req->kv.value, "NULL");
				returnNull = 1;
			} else {
				//printf("Valor do req->value: %s\n", (ret == NULL ? "NULL" : req->kv.value));
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

int consIndex = 0;
int freeIndex = 0;
//Pointer to the buffer
buffer_t** req_buffer;

void init_reqBuffer(){

	int i;
	req_buffer = (buffer_t**)malloc(sizeof(buffer_t*) * REQ_BUFF_SIZE);
	memset(req_buffer, NULL, sizeof(buffer_t*) * REQ_BUFF_SIZE);
	}


//Sends data to buffer
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
		freeIndex = (freeIndex + 1) % REQ_BUFF_SIZE;
		//printf("ProdutorPUT: Item introduzido no buffer\n");
	    sem_post(&mutex);
	sem_post(&fillCount);
 


	// Espera resposta do servidor
	sem_wait(&req->sem);
		
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

//Gets data from KOS
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
		freeIndex = (freeIndex + 1) % REQ_BUFF_SIZE;
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
		freeIndex = (freeIndex + 1) % REQ_BUFF_SIZE;
		//printf("ProdutorRemove: Item introduzido no buffer\n");
	    sem_post(&mutex);
	sem_post(&fillCount);
 
	// Espera resposta
	sem_wait(&req->sem);
		
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
		freeIndex = (freeIndex + 1) % REQ_BUFF_SIZE;
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

 
void consumer_server( void *ptr ) {
    buffer_t* req;

    while (1) {
	//printf("Consumidor: Entrou na tarefa by: %d\n", x);
        sem_wait(&fillCount);
            sem_wait(&mutex);
		req = req_buffer[consIndex];
		consIndex = (consIndex + 1) % REQ_BUFF_SIZE;
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

int lookup(char* key, char* value, KV_t* dump,int dim) {
	int i=0;
	for (;i<dim;i++) {
		if ( (strncmp(key,dump[i].key,KEY_SIZE)) &&  (strncmp(value,dump[i].value,KEY_SIZE) ) )
		 return 0;
	}
	return -1;
}


void *client_thread(void *arg) {

	char key[KEY_SIZE], value[KEY_SIZE], value2[KEY_SIZE];
	char* v;
	int i,dim;
	int client_id=*( (int*)arg);
	KV_t* dump;

// Check if shard is empty or already written. Check the first <key,value> usually inserted //
	i=NUM_EL-1;
	sprintf(key, "k-c%d-%d", client_id, i);
	sprintf(value, "val:%d", i);
	v=kos_get(client_id, client_id, key);

	if (v == NULL) {
		printf("Shard seems to be empty - Run all tests 1 to 6");
		// Otherwise goes directly to test 7 - the final getAllKeys //


	for (i=NUM_EL-1; i>=0; i--) {
		sprintf(key, "k-c%d-%d",client_id,i);
		sprintf(value, "val:%d",i);
		v=kos_put(client_id, client_id, key,value);
		DEBUG("C:%d  <%s,%s> inserted in shard %d. Prev Value=%s\n", client_id, key, value, i, ( v==NULL ? "<missing>" : v ) );
	}

	printf("------------------- %d:1/7 ENDED INSERTING -----------------------\n",client_id);

	for (i=0; i<NUM_EL; i++) {
		sprintf(key, "k-c%d-%d",client_id,i);
		sprintf(value, "val:%d",i);
		v=kos_get(client_id, client_id, key);
		if (strncmp(v,value,KEY_SIZE)!=0) {
			printf("Error on key %s value should be %s and was returned %s",key,value,v);
			exit(1);
		}
		DEBUG("C:%d  %s %s found in shard %d: value=%s\n", client_id, key, ( v==NULL ? "has not been" : "has been" ),client_id,
								( v==NULL ? "<missing>" : v ) );	
	}

	
	printf("------------------ %d:2/7 ENDED READING  ---------------------\n",client_id);


	dump=kos_getAllKeys(client_id, client_id, &dim);
	if (dim!=NUM_EL) {
		printf("TEST FAILED - SHOULD RETURN %d ELEMS AND HAS RETURNED %d",NUM_EL,dim);
		exit(-1);
	}
		
	for (i=0; i<NUM_EL; i++) {
		sprintf(key, "k%d",i);
		sprintf(value, "val:%d",i);
		if (lookup(key,value,dump, dim)!=0) {
			printf("TEST FAILED - Error on <%s,%s>, shard %d - not returned in dump\n",key,value,client_id);
			exit(-1);
		}			

	}


	printf("----------------- %d-3/7 ENDED GET ALL KEYS -------------------------\n",client_id);


	for (i=NUM_EL-1; i>=NUM_EL/2; i--) {
		sprintf(key, "k-c%d-%d",client_id,i);
		sprintf(value, "val:%d",i);
		v=kos_remove(client_id, client_id, key);
		if (strncmp(v,value,KEY_SIZE)!=0) {
			printf("Error when removing key %s value should be %s and was returned %s",key,value,v);
			exit(1);
		}
		DEBUG("C:%d  %s %s removed from shard %d. value =%s\n", client_id, key, ( v==NULL ? "has not been" : "has been" ),client_id,
								( v==NULL ? "<missing>" : v ) );
	}



	printf("----------------- %d-4/7 ENDED REMOVING -------------------------\n",client_id);


	for (i=0; i<NUM_EL; i++) {
		sprintf(key, "k-c%d-%d",client_id,i);
		sprintf(value, "val:%d",i);
		v=kos_get(client_id, client_id, key);
		if (i>=NUM_EL/2 && v!=NULL) {
			printf("Error when gettin key %s value should be NULL and was returned %s",key,v);
			exit(1);
		}
		if (i<NUM_EL/2 && strncmp(v,value,KEY_SIZE)!=0 ) {
			printf("Error on key %s value should be %s and was returned %s",key,value,v);
			exit(1);
		}
		DEBUG("C:%d  %s %s found in shard %d. value=%s\n", client_id, key, ( v==NULL ? "has not been" : "has been" ) ,client_id, ( v==NULL ? "<missing>" : v ) );
	}

	printf("----------------- %d-5/7 ENDED CHECKING AFTER REMOVE -----------------\n",client_id);


		for (i=0; i<NUM_EL; i++) {
			sprintf(key, "k-c%d-%d",client_id,i);
			sprintf(value, "val:%d",i*10);
			sprintf(value2, "val:%d",i*1);
			v=kos_put(client_id, client_id, key,value);

			if (i>=NUM_EL/2 && v!=NULL) {
				printf("Error when getting key %s value should be NULL and was returned %s",key,v);
				exit(1);
			}
			if (i<NUM_EL/2 && strncmp(v,value2,KEY_SIZE)!=0 ) {
				printf("Error on key %s value should be %s and was returned %s",key,value2,v);
				exit(1);
			}


			DEBUG("C:%d  <%s,%s> inserted in shard %d. Prev Value=%s\n", client_id, key, value, client_id, ( v==NULL ? "<missing>" : v ) );
		}


	printf("----------------- %d-6/7 ENDED 2nd PUT WAVE ----------------\n",client_id);


} // adaptation to getAllKeys from file //


		dump=kos_getAllKeys(client_id, client_id, &dim);
		if (dim!=NUM_EL) {
			printf("TEST FAILED - SHOULD RETURN %d ELEMS AND HAS RETURNED %d",NUM_EL,dim);
			exit(-1);
		}
			
		for (i=0; i<NUM_EL; i++) {
			sprintf(key, "k%d",i);
			sprintf(value, "val:%d",i*10);
			if (lookup(key,value,dump, dim)!=0) {
				printf("TEST FAILED - Error on <%s,%s>, shard %d - not returned in dump\n",key,value,client_id);
				exit(-1);
			}			

		}

	

	printf("----------------- %d-7/7 ENDED FINAL ALL KEYS CHECK ----------------------\n",client_id);

	return NULL;
}



int main(int argc, const  char* argv[] ) {

int i,s,ret;
	int* res;
	pthread_t* threads=(pthread_t*)malloc(sizeof(pthread_t)*NUM_CLIENT_THREADS);
	int* ids=(int*) malloc(sizeof(int)*NUM_CLIENT_THREADS);

	// this test uses NUM_CLIENT_THREADS shards, as each client executes commands in its own shard
	ret=kos_init(NUM_SERVER_THREADS,NUM_SERVER_THREADS,NUM_CLIENT_THREADS);

	//printf("KoS inited");

	if (ret!=0)  {
			printf("kos_init failed with code %d!\n",ret);
			return -1;
		}
		
	for (i=0; i<NUM_CLIENT_THREADS; i++) {	
		ids[i]=i;		
		
		if ( (s=pthread_create(&threads[i], NULL, &client_thread, &(ids[i])) ) ) {
			printf("pthread_create failed with code %d!\n",s);
			return -1;
		}
	}

	for (i=0; i<NUM_CLIENT_THREADS; i++) {	
               s = pthread_join(threads[i], (void**) &res);
               if (s != 0) {
                   printf("pthread_join failed with code %d",s);
			return -1;
		}
           }

	printf("\n--> TEST PASSED <--\n");

	return 0;
}
