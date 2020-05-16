//
// Created by harshal on 11/24/18.
//

#include "gloo/rendezvous/zookeeper_store.h"

#include <thread>
#include <iostream>
#include <string>
#include <string.h>
#include "gloo/common/error.h"
#include "gloo/common/logging.h"
#include "gloo/common/string.h"

#define ZDATALEN 1024 * 1024

namespace gloo {
  namespace rendezvous {

    static const std::chrono::seconds kWaitTimeout = std::chrono::seconds(60);

    ZookeeperStore::ZookeeperStore(const std::string &host, int port) {
      std::string hostAndPort = host + ":" + std::to_string(port);
      zookeeper_ = zookeeper_init(hostAndPort.c_str(), NULL, 20000, 0, 0, 0);
      GLOO_ENFORCE(zookeeper_ != nullptr);
    }

    //TODO: Need to free appropriate allocated structures
    ZookeeperStore::~ZookeeperStore() {

    }

    void ZookeeperStore::set(const std::string &key, const std::vector<char> &data) {
      std::cout << "inside set function of zookeeper" <<std::endl;

      std::string combinedKey = key;
      std::vector<std::string> prefixKeys;
      size_t slash_index;

      //split key by / to get individual znodes
      while ((slash_index = combinedKey.find("/")) != std::string::npos) {
        prefixKeys.push_back(combinedKey.substr(0, slash_index));
        combinedKey.erase(0, slash_index + 1);
      }
      prefixKeys.push_back(combinedKey);

      std::string pathBuf = prefixKeys.front();
      pathBuf.insert(0, "/");


      // creating prefix node(unique for a single run) if it does not exist
      if (ZNONODE == zoo_exists(zookeeper_, pathBuf.c_str(), 0, NULL)) {
        if (ZOK == zoo_create(zookeeper_, pathBuf.c_str(), NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0)) {
          //fprintf(stderr, "%s created!\n", pathBuf.c_str());
        } else {
          fprintf(stderr, "Error Creating %s!\n", pathBuf.c_str());
          GLOO_THROW_INVALID_OPERATION_EXCEPTION("Error Creating %s!\n", pathBuf.c_str());
        }
      }
      pathBuf = pathBuf + "/" + prefixKeys.back();

      // creating node for key in zookeeper(each process creates its own node)
      if (ZOK == zoo_create(zookeeper_, pathBuf.c_str(), NULL, -1, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0)) {
        //fprintf(stderr, "%s created!\n", pathBuf.c_str());
      } else {
        fprintf(stderr, "Error Creating %s!\n", pathBuf.c_str());
        GLOO_THROW_INVALID_OPERATION_EXCEPTION("Error Creating %s!\n", pathBuf.c_str());
      }

      std::cout <<"trying to set data" << std::endl;

      if (ZOK != zoo_set(zookeeper_, pathBuf.c_str(), data.data(), (int) data.size(), -1)) {
        fprintf(stderr, "Error setting value for %s!\n", pathBuf.c_str());
        GLOO_THROW_INVALID_OPERATION_EXCEPTION("Error setting value for %s!\n", pathBuf.c_str());
      }else{
        //fprintf(stderr, "Set value %s for %s\n", data.data(), pathBuf.c_str());
      }
      std::cout <<"set data of size "<<data.size() << std::endl;
    }

    std::vector<char> ZookeeperStore::get(const std::string &key) {
      //wait until other process sets value for this key
      wait({key});

      std::cout << "inside get function of zookeeper" <<std::endl;
      std::string pathBuf = key;
      pathBuf.insert(0, "/");

      char *keyVal = (char*) malloc(ZDATALEN*sizeof(char));
      int keyValSize = ZDATALEN;

      //get value set for this key
      if (ZOK != zoo_get(zookeeper_, pathBuf.c_str(), 0, keyVal, &keyValSize, NULL)) {
        GLOO_THROW_INVALID_OPERATION_EXCEPTION("Error getting value for %s!\n", pathBuf.c_str());
      }
      std::cout<< "get string returned size "<< keyValSize << std::endl;
      std::vector<char> result(keyVal, keyVal + keyValSize);
      return result;

    }

    bool ZookeeperStore::check(const std::vector<std::string> &keys) {

      //check if key is set by other process. Return true only if all keys are set
      for(std::string key : keys){
        std::string tempkey = key;
        tempkey.insert(0, "/");
        if (ZNONODE == zoo_exists(zookeeper_, tempkey.c_str(), 0, NULL)){
          //std::cout<<"search key "<<tempkey<<" not found" << std::endl;
          return false;
        }
      }
      return true;
    }

    void ZookeeperStore::wait(
        const std::vector<std::string> &keys,
        const std::chrono::milliseconds &timeout) {

      std::cout << "performing wait for zookeeper store" << std::endl;
      // Polling is fine for the typical rendezvous use case, as it is
      // only done at initialization time and  not at run time.
      const auto start = std::chrono::steady_clock::now();
      while (!check(keys)) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start);
        if (timeout != kNoTimeout && elapsed > timeout) {
          GLOO_THROW_IO_EXCEPTION(GLOO_ERROR_MSG(
              "Wait timeout for key(s): ", ::gloo::MakeString(keys)));
        }
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

    }

  } // namespace rendezvous
} // namespace gloo