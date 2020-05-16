/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stddef.h>
#include <string.h>
#include <gloo/common/logging.h>

#include "gloo/algorithm.h"
#include "gloo/context.h"
#include <stdio.h>
#include <string>
#include <fstream>
#include "gloo/rendezvous/zookeeper_store.h"


namespace gloo {

  template<typename T>
  class AllreduceRing : public Algorithm {
  public:
    AllreduceRing(
        const std::shared_ptr<Context> &context,
        const std::vector<T *> &ptrs,
        const int count,
        const ReductionFunction<T> *fn = ReductionFunction<T>::sum)
        : Algorithm(context),
          ptrs_(ptrs),
          count_(count),
          bytes_(count_ * sizeof(T)),
          fn_(fn) {
      inbox_ = static_cast<T *>(malloc(bytes_));
      outbox_ = static_cast<T *>(malloc(bytes_));
      tempBuf_ = static_cast<T *>(malloc(bytes_));

      skipNeighbor = 2;

      numRounds = this->contextSize_ - 1;

      if (this->contextSize_ == 1) {
        return;
      }


      auto &leftPair = this->getLeftPair();
      auto &rightPair = this->getRightPair();
      auto slot = this->context_->nextSlot();


      // Buffer to send to (rank+1).
      sendDataBuf_ = rightPair->createSendBuffer(slot, outbox_, bytes_);

      // Buffer that (rank-1) writes to.
      recvDataBuf_ = leftPair->createRecvBuffer(slot, inbox_, bytes_);

      // Dummy buffers for localized barrier.
      // Before sending to the right, we only need to know that the node
      // on the right is done using the inbox that's about to be written
      // into. No need for a global barrier.
      auto notificationSlot = this->context_->nextSlot();
      //auto notificationSlot = 0;
      sendNotificationBuf_ =
          leftPair->createSendBuffer(notificationSlot, &dummy_, sizeof(dummy_));
      recvNotificationBuf_ =
          rightPair->createRecvBuffer(notificationSlot, &dummy_, sizeof(dummy_));
    }

    virtual ~AllreduceRing() {
      if (inbox_ != nullptr) {
        free(inbox_);
      }
      if (outbox_ != nullptr) {
        free(outbox_);
      }
    }

    bool checkAliveness(int rank) {
      activeNodes.open("status.txt");
      while (getline(activeNodes, proc)) {
        int cur_rank = stoi(proc);
        if (cur_rank == rank) {
          activeNodes.close();
          return true;
        }
      }
      activeNodes.close();
      return false;
    }


    void fixLeftPair() {

      auto rank = (context_->size + context_->rank - skipNeighbor) % context_->size;
      while (rank != this->contextRank_) {
        if (checkAliveness(rank)) {
          break;
        }
        skipNeighbor++;
        rank = (context_->size + context_->rank - skipNeighbor) % context_->size;
      }
      if (rank == this->contextRank_) {
        GLOO_ERROR_MSG("no left pair to communicate");
      }

      GLOO_ENFORCE(context_->getPair(rank), "pair missing (index ", rank, ")");
      auto &leftPair = context_->getPair(rank);
      auto slot = context_->nextSlot();


      recvDataBuf_ = leftPair->createRecvBuffer(slot, inbox_, bytes_);
      auto notificationSlot = context_->nextSlot();

      sendNotificationBuf_ = leftPair->createSendBuffer(notificationSlot, &dummy_, sizeof(dummy_));

      try {
        recvDataBuf_->waitRecv();
      } catch (::gloo::IoException e) {
        GLOO_ERROR_MSG("Failed on wait recv after fixing the left pair");
      }

      try {
        recvNotificationBuf_->waitRecv();
      } catch (::gloo::IoException e) {
        GLOO_ERROR_MSG("Failed on wait recv notification after fixing the left pair");
      }

      skipNeighbor++;
    }

    void fixRightPair() {


      auto rank = (context_->rank + skipNeighbor) % context_->size;

      while (rank != this->contextRank_) {
        if (checkAliveness(rank)) {
          break;
        }
        skipNeighbor++;
        rank = (context_->rank + skipNeighbor) % context_->size;
      }
      if (rank == this->contextRank_) {
        GLOO_ERROR_MSG("No right pair to communicate");
      }

      GLOO_ENFORCE(context_->getPair(rank), "pair missing (index ", rank, ")");
      auto &rightPair = context_->getPair(rank);
      auto slot = context_->nextSlot();


      sendDataBuf_ = rightPair->createSendBuffer(slot, outbox_, bytes_);
      auto notificationSlot = context_->nextSlot();
      recvNotificationBuf_ =
          rightPair->createRecvBuffer(notificationSlot, &dummy_, sizeof(dummy_));

      memcpy(outbox_, tempBuf_, bytes_);



      // Initiate write to inbox of node on the right
      sendDataBuf_->send();

      try {
        // Wait for outbox write to complete
        sendDataBuf_->waitSend();
      } catch (::gloo::IoException e) {
        throw e;
      }


      memcpy(outbox_, inbox_, bytes_);

      //resending notification to left pair
      sendNotificationBuf_->send();

      try {
        // Wait for notification from node on the right
        recvNotificationBuf_->waitRecv();
      } catch (::gloo::IoException e) {
        GLOO_ERROR_MSG("Failed on wait recv notification after fixing the right pair");
      }

      skipNeighbor++;
    }


    void run() {

      if (this->contextRank_ == 2) {
        exit(1);
      }

      // Reduce specified pointers into ptrs_[0]
      for (int i = 1; i < ptrs_.size(); i++) {
        fn_->call(ptrs_[0], ptrs_[i], count_);
      }


      // Intialize outbox with locally reduced values
      memcpy(outbox_, ptrs_[0], bytes_);

      for (int round = 0; round < numRounds; round++) {
        // Initiate write to inbox of node on the right
        sendDataBuf_->send();

        try {
          // Wait for inbox write from node on the left
          recvDataBuf_->waitRecv();
        } catch (::gloo::IoException e) {
          if (!checkAliveness((context_->size + context_->rank - (skipNeighbor - 1)) % context_->size)) {
            fixLeftPair();
          } else {
            GLOO_ERROR_MSG("IoException received while left pair is still alive");
          }
          numRounds--;
        }

        memcpy(tempBuf_, outbox_, bytes_);

        // Reduce
        fn_->call(ptrs_[0], inbox_, count_);

        try {
          // Wait for outbox write to complete
          sendDataBuf_->waitSend();

        } catch (::gloo::IoException e) {
          throw e;
        }

        // Prepare for next round if necessary
        if (round < (numRounds - 1)) {
          memcpy(outbox_, inbox_, bytes_);
        }


        // Send notification to node on the left that
        // this node is ready for an inbox write.
        sendNotificationBuf_->send();

        try {

          // Wait for notification from node on the right
          recvNotificationBuf_->waitRecv();

        } catch (::gloo::IoException e) {

          if (!checkAliveness((context_->rank + (skipNeighbor - 1)) % context_->size)) {
            fixRightPair();

          } else {
            GLOO_ERROR_MSG("IoException received while right pair is still alive");
          }

          numRounds--;
        }

        //reading file for active nodes
        int actCt = 0;
        activeNodes.open("status.txt");

        while (getline(activeNodes, proc)) {
          actCt++;
        }

        activeNodes.close();

        //indirect node checks the status of the ring and adjusts numRounds for its process
        if (numRounds >= actCt) {

          int diffRounds = numRounds - actCt;
          numRounds = numRounds - diffRounds - 1;

          try {
            recvNotificationBuf_->waitRecv();
          } catch (::gloo::IoException e) {
            throw e;
          }
          sendNotificationBuf_->send();
        }

      }

      // Broadcast ptrs_[0]
      for (int i = 1; i < ptrs_.size(); i++) {
        memcpy(ptrs_[i], ptrs_[0], bytes_);
      }

    }

  protected:
    std::vector<T *> ptrs_;
    const int count_;
    const int bytes_;

    const ReductionFunction<T> *fn_;

    //changed to class member so that the number of rounds carry over to the next iteration
    int numRounds;


    T *inbox_;
    T *outbox_;

    //extra buffer to resend lost data to right pair
    T *tempBuf_;

    std::unique_ptr<transport::Buffer> sendDataBuf_;
    std::unique_ptr<transport::Buffer> recvDataBuf_;

    int dummy_;
    std::unique_ptr<transport::Buffer> sendNotificationBuf_;
    std::unique_ptr<transport::Buffer> recvNotificationBuf_;

    //variable to know how many nodes to skip incase of a fault
    int skipNeighbor;

    //file stream to get the active nodes
    std::ifstream activeNodes;

    //variable to hold the value read from the file
    std::string proc;

  };

} // namespace gloo
