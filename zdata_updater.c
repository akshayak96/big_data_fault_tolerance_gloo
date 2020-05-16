   #include <time.h>
   #include <stdio.h>
   #include <errno.h>
   #include <string.h>
   #include <stdlib.h>
   #include <unistd.h>
   #include <zookeeper.h>
   #include <stdio.h>
   /* ZooKeeper Znode Data Length (1MB, the max supported) */
   #define ZDATALEN    1024 * 1024
   static char *host_port;
   static char *host_port_2;
   static char *host_port_3;
   static char *zoo_path = "/P0";
   static char *zoo_path_2 = "/P1";
   static char *zoo_path_3 = "/P2";
   static zhandle_t *zh;
   static zhandle_t *zh_2;
   static zhandle_t *zh_3;
   static int is_connected;
/**
* Watcher function for connection state change events
*/
void connection_watcher(zhandle_t *zzh, int type, int state,
  const char *path, void* context)
{
  if (type == ZOO_SESSION_EVENT)
  {
    if (state == ZOO_CONNECTED_STATE)
    {
      is_connected = 1;
    }
    else
    {
      is_connected = 0;
    }
  } 
}
int main(int argc, char *argv[])
{
  char zdata_buf[128];
  char zdata_buf_2[128];
  char zdata_buf_3[128];
  struct tm *local;
  time_t t;
  struct tm *local2;
  time_t t2;
  double diff_t;
  double timeout=5;
  int q=1;
  if (argc != 4)
  {
    fprintf(stderr, "USAGE: %s host:port\n", argv[0]);
    exit(EXIT_FAILURE);
  }
  host_port = argv[1];
  host_port_2 = argv[2];
  host_port_3 = argv[3];

  zh = zookeeper_init(host_port, connection_watcher,2000, 0, 0, 0);

  zh_2 = zookeeper_init(host_port_2, connection_watcher,2000, 0, 0, 0);

  zh_3 = zookeeper_init(host_port_3, connection_watcher,2000, 0, 0, 0);

       if ((zh == NULL)|| (zh_2 == NULL)|| (zh_3 == NULL))
       {
            fprintf(stderr,"Error connecting to ZooKeeper server[%d]!\n", errno);
            exit(EXIT_FAILURE);
       }
       sleep(3); /* Sleep a little for connection to complete */
      
       if (is_connected)
       {
	 //for /P0 Node      
         if (ZNONODE == zoo_exists(zh, zoo_path, 0, NULL))
         {
           fprintf(stderr, "%s doesn't exist! \ Please start zdata_watcher.\n", zoo_path);
           exit(EXIT_FAILURE);
         }


	 //for P1 node
            if (ZNONODE == zoo_exists(zh, zoo_path_2, 0, NULL))
         {
           fprintf(stderr, "%s doesn't exist! \ Please start zdata_watcher.\n", zoo_path_2);
           exit(EXIT_FAILURE);
         }


         //for P2 node
            if (ZNONODE == zoo_exists(zh, zoo_path_3, 0, NULL))
         {
           fprintf(stderr, "%s doesn't exist! \ Please start zdata_watcher.\n", zoo_path_3);
           exit(EXIT_FAILURE);
         }

         
         //Timestamps and node availability of /P0 node	    

          t = time(NULL);
          local = localtime(&t);
          fprintf(stderr,"************************************************************\n"); 
	  fprintf(stderr,"Timeout = %f seconds\n",timeout);
	  while(1)
       {
            fprintf(stderr,"\nLocal Timestamp from P0:%s\n",asctime(local)); 
	    t2 = time(NULL);
	    local2 = localtime(&t2);
	    fprintf(stderr,"Next Timestamp from P0:%s\n",asctime(local2));
	    diff_t = difftime(t2, t);
	    fprintf(stderr,"Difference between the Timestamps for P0: %f\n\n",diff_t);
	    fprintf(stderr,"**********************************************************\n");
            memset(zdata_buf,'\0',strlen(zdata_buf));
	    if(diff_t>timeout)
	 {	   
           strcpy(zdata_buf, "ZNODE UNAVAILABLE");
	 } 
	   else
          {
           strcpy(zdata_buf, "ZNODE AVAILABLE");
         }		   
           //zoo_set: write data from zdata_buf to the znode in the zoo_path
	   if (ZOK != zoo_set(zh, zoo_path, zdata_buf, strlen(zdata_buf),-1))
	   {	   
		  
             fprintf(stderr, "Error in write at %s!\n", zoo_path);
          
	   }	   
                   t=t2;
		   local = localtime(&t);
          	   sleep(q);
		   q++;
        
          //Timestamps and node availability of /P1 node  
           t2 = time(NULL);
           local2 = localtime(&t2);
           fprintf(stderr,"Next Timestamp from P1:%s\n",asctime(local2));
           diff_t = difftime(t2, t);
           fprintf(stderr,"Difference between the Timestamps for P1: %f\n\n",diff_t);
	   fprintf(stderr,"**********************************************************\n");
           memset(zdata_buf_2,'\0',strlen(zdata_buf_2));
           if(diff_t>timeout)
         {
           strcpy(zdata_buf_2, "ZNODE UNAVAILABLE");
         }
           else
          {
           strcpy(zdata_buf_2, "ZNODE AVAILABLE");
         }
           //zoo_set: write data from zdata_buf to the znode in the zoo_path
           if (ZOK != zoo_set(zh, zoo_path_2, zdata_buf_2, strlen(zdata_buf_2),-1))
           {
                 
             fprintf(stderr, "Error in write at %s!\n", zoo_path_2);
          
           }
                   t=t2;
                   local = localtime(&t);
                   sleep(q);
                   q++;
    
       
           //Timestamps and node availability of /P2 node  

	   t2 = time(NULL);
           local2 = localtime(&t2);
           fprintf(stderr,"Next Timestamp from P2:%s\n",asctime(local2));
           diff_t = difftime(t2, t);
           fprintf(stderr,"Difference between the Timestamps for P2: %f\n\n",diff_t);
	   fprintf(stderr,"**********************************************************\n");
           memset(zdata_buf_3,'\0',strlen(zdata_buf_3));
           if(diff_t>timeout)
         {
           strcpy(zdata_buf_3, "ZNODE UNAVAILABLE");
         }
           else
          {
           strcpy(zdata_buf_3, "ZNODE AVAILABLE");
         }
           //zoo_set: write data from zdata_buf to the znode in the zoo_path
           if (ZOK != zoo_set(zh, zoo_path_3, zdata_buf_3, strlen(zdata_buf_3),-1))
           {
               
             fprintf(stderr, "Error in write at %s!\n", zoo_path_3);
          
           }
                   t=t2;
                   local = localtime(&t);
                   sleep(q);
                   q++;
        }
    
      }
           return 0;
}


