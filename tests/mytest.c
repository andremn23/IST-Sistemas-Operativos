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

#define HT_SIZE 20
#define REQ_BUFF_SIZE 20
#define MAX_CHARS 20
#define NUM_EL 200
#define NUM_SHARDS 4
#define NUM_CLIENT_THREADS 3 
#define NUM_SERVER_THREADS 3
#define BUF_SIZE 5

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
FILE *file;

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
sem_t write_mutex;

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
        printf("Entrou no kos_init\n");
	buffer = (buffer_t*)malloc(sizeof(buffer_t) * buf_size);
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
        sem_init(&write_mutex, 0, 1);  
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

void fprintfShards(list_t *list)
{
   lst_iitem_t* tmp;

   if(list)
   {
      tmp = list->first;

      if(!tmp)
         fprintf(file, "(Vazia)");

      while(tmp){
	 fprintf(file, "(%s, %s), ", tmp->value->key, tmp->value->value);
         tmp = tmp->next;
      }
   }
}

void writeShard(int shardId){
	int j;
	fprintf(file,"A imprimir shard: %d", shardId); /*writes*/
	for(j= 0; j < HT_SIZE; j++){
			fprintf(file, "\nclientId %d: ", j);
			fprintfShards(&k_shard[shardId].lists[j]);			
		}


}


void writeToFile(){

	char filename[20];
	int shardId, i;

	for (i= 0; i< 4; i++){
		sprintf(filename, "fshard%d.txt", i);
		file = fopen(filename,"w"); /* apend file (add text to //a+ mantem o que esta no ficheiro*/
		//fprintf(file,"%s","teste");
		writeShard(i);
		fclose(file); /*done!*/

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


void producer_client(void *arg) {

	char key[KV_SIZE], value[KV_SIZE], value2[KV_SIZE];
	char* v;
	int i;
	int dim;
	KV_t* teste;
	int client_id=*( (int*)arg);
	//for (j=0; j< 5; j++){
	//sprintf(key, "k-c%d-%d",client_id, j);
	//sprintf(value, "val:%d",j);
	//sprintf(key, "k-c\0");
	//sprintf(value, "val:%d\0",j);
	//v=kos_put(client_id, j, key, value);
	
	sprintf(key, "key%d\0", client_id);
	sprintf(value, "val%d\0", *((int*)arg));
	kos_put(1, 1, "key1", "value1");
	kos_put(1, 1, "key4", "value4");
	kos_put(1, 1, "key5", "value5");
	kos_put(0, 0, "key2", "value2");
	kos_put(2, 2, "key3", "value3");
	kos_put(client_id, 3, key, "value3");
	sem_wait(&write_mutex);
	writeToFile();
	sem_post(&write_mutex);
	int var;
	char* key = "teste\0";
	var = hash(key);
	printf("Hash: %d\n", var);
	//kos_list(k_shard);
	//kos_put(1, 1, "key2", "value2");
	//kos_put(1, 1, "key3", "value3");
	//kos_put(1, 1, "key4", "value4");
	//kos_put(0, 0, "key8", "value8");
	//kos_put(0, 0, "key9", "value9");
	//kos_put(0, 0, "key10", "value10");
	//teste = kos_getAllKeys(0, 0, &dim);
	//printf("dim: %d\n", dim);

	//if(!teste){printf("E Null\n");} else {
	//printf("Teste: %s\n", teste[0].value);}
	/*kos_get(0, 1, "key10");
	kos_get(0, 0, "key5");
	kos_get(1, 0, "key3");
	kos_get(1, 1, "key7");
	kos_remove(0, 0, "key412");
	kos_remove(1, 1, "key4");
	kos_remove(1, 0, "key3");
	kos_remove(1, 1, "key4");
	kos_put(0, 2, "key3", "value3");*/
	//kos_put(0, 3, "key4", "value4");
	//kos_put(0, 0, "key1", "value3");

	//kos_get(0, 0, "key1");
	//kos_remove(0, 0, "key1");
	//kos_list(k_shard);	
	//}
        //if (v)
	//    printf("Resposta do servidor: %s\n", v);
        //else
        //    printf("Resposta do servidor: NULL\n");
	//}

/*	char key[KV_SIZE], value[KV_SIZE], value2[KV_SIZE];
	char* v;
	int i,j;
	int client_id=*( (int*)arg);


	for (j=NUM_SHARDS-1; j>=0; j--) {	
		for (i=NUM_EL; i>=0; i--) {
			sprintf(key, "k-c%d-%d",client_id,i);
			sprintf(value, "val:%d",i);
			v=kos_put(client_id, j, key,value);
			DEBUG("C:%d  <%s,%s> inserted in shard %d. Prev Value=%s\n", client_id, key, value, j, ( v==NULL ? "<missing>" : v ) );
		}
	}

	printf("------------------- %d:1/6 ENDED INSERTING -----------------------\n",client_id);*/


}



int main(int argc, const  char* argv[] ) {

int s,ret, i;
	int* res;
	pthread_t* threads=(pthread_t*)malloc(sizeof(pthread_t)*NUM_CLIENT_THREADS);
	int* ids=(int*) malloc(sizeof(int)*NUM_CLIENT_THREADS);

	ret=kos_init(NUM_SERVER_THREADS, BUF_SIZE, NUM_SHARDS);


   	int *idClient = (int*)malloc(sizeof(int)* NUM_CLIENT_THREADS);
	pthread_t* clientThreads = (pthread_t*)malloc(sizeof(pthread_t) * NUM_CLIENT_THREADS);


        
   for (i=0; i<NUM_CLIENT_THREADS; i++) {	
		idClient[i]=i;		
		
		if ( (s=pthread_create(&clientThreads[i], NULL, &producer_client, &(idClient[i])) ) ) {
			printf("pthread_create failed with code %d!\n",s);
			return -1;
		}
	}

	for (i=0; i<NUM_CLIENT_THREADS; i++) {	
               s = pthread_join(clientThreads[i], (void**) &res);
               if (s != 0) {
                   printf("pthread_join failed with code %d",s);
			return -1;
		}
           }
	//kos_list(k_shard);
	


	return 0;

}
