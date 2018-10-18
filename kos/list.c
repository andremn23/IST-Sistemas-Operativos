/*
 * list.c - implementation of the integer list functions 
 */

#include <stdlib.h>
#include <stdio.h>
#include "list.h"

void item_print(Pair pair)
{
   printf("   Key: %s, Value: %s\n", pair->key, pair->value);
}

int item_equals(Pair item1, ItemID valueID)
{
   if(strcmp(item1->key, valueID) == 0)
      return EQUAL;
   
   return NOT_EQUAL;
}

/*void item_free(Pair pair)
{
   if(pair)
      task_free(pair);
}*/

/*void task_free(task_t* task)
{
   if(task)
   {
      free(task->name);
      free(task);
   }
}*/

list_t* lst_new()
{
   list_t *list;
   list = (list_t*) malloc(sizeof(list_t));
   list->first = NULL;
   return list;
}

/*void lst_destroy(list_t *list)
{
   lst_iitem_t* curr = list->first;
   lst_iitem_t* rem;

   while(curr)
   {
      rem = curr;
      curr = curr->next;
      item_free(rem->value);
      free(rem);
   }
   
   free(list);
}*/

void lst_insert(list_t *list, Pair value)
{
   lst_iitem_t* item = (lst_iitem_t*)malloc(sizeof(lst_iitem_t));
   lst_iitem_t* tmp = NULL;

   if(item)
   {
      item->value = value;
      item->next = NULL;
   }

   if(list)
      tmp = list->first;

   while(tmp && tmp->next)
      tmp = tmp->next;
   
   if(tmp)
      tmp->next = item;
   else
      list->first = item;
}

int lst_remove(list_t *list, ItemID valueID)
{
   lst_iitem_t* item = NULL;
   lst_iitem_t* prev = NULL;
   
   if(list)
      item = list->first;

   while(item && !item_equals(item->value, valueID))
   {
      prev = item;
      item = item->next;
   }

   if(item)
   {
      if(prev)
         prev->next = item->next;
      else
         list->first = item->next;

      //item_free(item->value);
      //free(item);
      return FOUND;
   }else
      return NOT_FOUND;
}

void lst_print(list_t *list)
{
   lst_iitem_t* tmp;

   if(list)
   {
      tmp = list->first;

      if(!tmp)
         printf("(Vazia)\n");

      while(tmp){
         item_print(tmp->value);
         tmp = tmp->next;
      }
   }
}
