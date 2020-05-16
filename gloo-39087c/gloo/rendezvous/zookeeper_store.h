//
// Created by harshal on 11/24/18.
//


#pragma once

#include <string>
#include <vector>
#include <zookeeper.h>

#include "gloo/config.h"
#include "gloo/rendezvous/store.h"

/*// Check that configuration header was properly generated
#if !GLOO_USE_ZOOKEEPER
#error "Expected GLOO_USE_ZOOKEEPER to be defined"
#endif*/


namespace gloo {
  namespace rendezvous {

    class ZookeeperStore : public Store {
    public:
      explicit ZookeeperStore(const std::string &host, int port = 6379);

      virtual ~ZookeeperStore();

      virtual void set(const std::string &key, const std::vector<char> &data)
      override;

      virtual std::vector<char> get(const std::string &key) override;

      bool check(const std::vector<std::string> &keys);

      virtual void wait(const std::vector<std::string> &keys) override {
        wait(keys, Store::kDefaultTimeout);
      }

      virtual void wait(
          const std::vector<std::string> &keys,
          const std::chrono::milliseconds &timeout) override;

    protected:
      zhandle_t *zookeeper_;
      int datalen;
    };

  } // namespace rendezvous
} // namespace gloo


