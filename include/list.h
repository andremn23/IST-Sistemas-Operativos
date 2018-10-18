/*
 * list.h - definitions and declarations of the integer list 
 */

#ifndef LIST_H
#define LIST_H

#include <stdlib.h>
#include <stdio.h>
#include "kos_client.h"


#define NOT_FOUND 0
#define FOUND 1

#define NOT_EQUAL 0 
#define EQUAL 1


/* Methods and types of list specialization */
typedef KV_t* Pair;
typedef char* ItemID;

//void item_print(Item item);
//int item_equals(Item item1, ItemID itemID);
//void item_free(Item item);

/* lst_iitem - each element of the list points to the next element */
typedef struct lst_iitem {
   Pair value;
   struct lst_iitem* next;
} lst_iitem_t;

/* list_t */
typedef struct {
   lst_iitem_t* first;
} list_t;

/* lst_new - allocates memory for list_t and initializes it */
list_t* lst_new();

/* lst_destroy - free memory of list_t and all its items */
//void lst_destroy(list_t*);

/* lst_insert - insert a new item with value 'value' in list 'list' */
void lst_insert(list_t *list, Pair value);

/* lst_remove - remove first item of value 'value' from list 'list' */
//int lst_remove(list_t *list, ItemID valueID);

/* lst_print - print the content of list 'list' to standard output */
//void lst_print(list_t *list);

#endif
