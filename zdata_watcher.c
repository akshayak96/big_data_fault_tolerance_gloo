 #include <stdio.h>
   #include <errno.h>
   #include <string.h>
   #include <stdlib.h>
   #include <unistd.h>
   #include <zookeeper.h>
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
   static char *watcher_ctx = "ZooKeeper Data Watcher";
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
else {
         is_connected = 0;
       }
} }
   /**
   * Data Watcher function for /P0, /P1 and /P2  nodes
   */
   static void
   data_watcher(zhandle_t *wzh, int type, int state, const char
     *zpath, void *watcher_ctx)
   {
     char *zoo_data = malloc(ZDATALEN * sizeof(char));
          char *zoo_data_2 = malloc(ZDATALEN * sizeof(char));
     char *zoo_data_3 = malloc(ZDATALEN * sizeof(char));

     int zoo_data_len = ZDATALEN;
     if (state == ZOO_CONNECTED_STATE)
	     {
    if (type == ZOO_CHANGED_EVENT && strcmp(zpath,zoo_path)==0)
    {
      /* Get the updated data and reset the watch for P0 node */
      zoo_wget(wzh, zoo_path, data_watcher,
      (void *)watcher_ctx, zoo_data, &zoo_data_len, NULL);
      fprintf(stderr, "!!! Data Change Detected !!!\n\n");
      fprintf(stderr, "%s at %s\n\n", zoo_data,zoo_path);
      fprintf(stderr, "*********************************************\n");
   }

    /* Get the updated data and reset the watch for P1 node */


    else if(type == ZOO_CHANGED_EVENT && strcmp(zpath,zoo_path_2)==0)
     {	    
      zoo_wget(wzh, zoo_path_2, data_watcher,
      (void *)watcher_ctx, zoo_data_2, &zoo_data_len, NULL);
      fprintf(stderr, "!!! Data Change Detected !!!\n\n");
      fprintf(stderr, "%s at %s\n\n", zoo_data_2,zoo_path_2);
      fprintf(stderr, "*********************************************\n");

    }
    /* Get the updated data and reset the watch for P2 node */

    else
    {	    
      zoo_wget(wzh, zoo_path_3, data_watcher,
      (void *)watcher_ctx, zoo_data_3, &zoo_data_len, NULL);
      fprintf(stderr, "!!! Data Change Detected !!!\n\n");
      fprintf(stderr, "%s at %s\n\n", zoo_data_3,zoo_path_3);
      fprintf(stderr, "*********************************************\n");

    }
} }

int main(int argc, char *argv[])
{
  int zdata_len;
  char *zdata_buf = NULL; //data buffer for /P0 node
  char *zdata_buf_2 = NULL; //data buffer for /P1 node
  char *zdata_buf_3 = NULL; //data buffer for /P2 node

  if (argc != 4)
  {
    fprintf(stderr, "USAGE: %s host:port\n", argv[0]);
    exit(EXIT_FAILURE);
  }
  host_port = argv[1];
  host_port_2 = argv[2];
  host_port_3 = argv[3];
  //Start a new session for /P0
  zh = zookeeper_init(host_port, connection_watcher,
    2000, 0, 0, 0);
  //Start a new session for /P1
  zh_2 = zookeeper_init(host_port_2, connection_watcher,
    2000, 0, 0, 0);
  //Start a new session for /P2
  zh_3 = zookeeper_init(host_port_3, connection_watcher,
    2000, 0, 0, 0);

  if ((zh == NULL)|| (zh_2 == NULL)|| (zh_3 == NULL))
  {
    fprintf(stderr,
      "Error connecting to ZooKeeper server[%d]!\n", errno);
    exit(EXIT_FAILURE);
  }
while (1) 
 {
    if (is_connected)
    {
      zdata_buf = (char *)malloc(ZDATALEN * sizeof(char));
      zdata_buf_2 = (char *)malloc(ZDATALEN * sizeof(char));
      zdata_buf_3 = (char *)malloc(ZDATALEN * sizeof(char));
      
      //FOR /P0 Node
      
      //Create a parent node if it does not exist
      if (ZNONODE == zoo_exists(zh, zoo_path, 0, NULL))
      {
        if (ZOK == zoo_create( zh, zoo_path, NULL, -1,
             & ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0))
           {
             fprintf(stderr, "%s created!\n", zoo_path);
           } 
	else 
	   {
             fprintf(stderr,
             "Error Creating %s!\n", zoo_path);
             exit(EXIT_FAILURE);
           } 
      }
            //if the operation of setting the watch on the znode mentioned in the zoo_path is not successful, send error msg
         if (ZOK != zoo_wget(zh, zoo_path, data_watcher,
           watcher_ctx, zdata_buf, &zdata_len, NULL))
         {
           fprintf(stderr, "Error setting watch at %s!\n", zoo_path);
         }
      
	  //FOR /P1 Node


           if (ZNONODE == zoo_exists(zh, zoo_path_2, 0, NULL))
      {
        if (ZOK == zoo_create( zh, zoo_path_2, NULL, -1,
             & ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0))
           {
             fprintf(stderr, "%s created!\n", zoo_path_2);
           }
        else
           {
             fprintf(stderr,
             "Error Creating %s!\n", zoo_path_2);
             exit(EXIT_FAILURE);
           }
      }
            //if the operation of setting the watch on the znode mentioned in the zoo_path is not successful, send error msg
         if (ZOK != zoo_wget(zh, zoo_path_2, data_watcher,
           watcher_ctx, zdata_buf_2, &zdata_len, NULL))
         {
           fprintf(stderr, "Error setting watch at %s!\n", zoo_path_2);
         }

             //FOR /P2 Node


             if (ZNONODE == zoo_exists(zh, zoo_path_3, 0, NULL))
      {
        if (ZOK == zoo_create( zh, zoo_path_3, NULL, -1,
             & ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0))
           {
             fprintf(stderr, "%s created!\n", zoo_path_3);
           }
        else
           {
             fprintf(stderr,
             "Error Creating %s!\n", zoo_path_3);
             exit(EXIT_FAILURE);
           }
      }
            //if the operation of setting the watch on the znode mentioned in the zoo_path is not successful, send error msg
         if (ZOK != zoo_wget(zh, zoo_path_3, data_watcher,
           watcher_ctx, zdata_buf_3, &zdata_len, NULL))
         {
           fprintf(stderr, "Error setting watch at %s!\n", zoo_path_3);
         }
	 

          	 pause();
    }
 }
     free(zdata_buf);
     free(zdata_buf_2);
     free(zdata_buf_3);

return 0;
}
