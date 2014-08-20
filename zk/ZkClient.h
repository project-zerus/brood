/**
 * @author Huahang Liu (huahang@zerus.co)
 * @date 2014-08-18
 */

#ifndef ZERUS_BROOD_ZK_ZK_CLIENT_H
#define ZERUS_BROOD_ZK_ZK_CLIENT_H

#include <functional>
#include <memory>
#include <string>

#include <boost/noncopyable.hpp>

#include "toft/system/threading/mutex.h"

#include "cpp-btree/btree_map.h"

#include "folly/FBString.h"

#include "zookeeper/zookeeper.h"

namespace zerus {
namespace brood {
namespace zk {

typedef std::function<void(const folly::fbstring& value, const bool deleted)>
DataChangeCallback;

class ZHandleDeleter {
public:
  void operator() (zhandle_t* zhandle) const;
};

class ZkClient : boost::noncopyable {
public:
  ZkClient() {}

  virtual ~ZkClient() {}

  void connect(const folly::fbstring& serverList);

  void createEphemeral(
    const folly::fbstring& path,
    const folly::fbstring& data
  );

  folly::fbstring createEphemeralSequence(
    const folly::fbstring& path,
    const folly::fbstring& data
  );

  void subscribeDataChanges(
    const folly::fbstring& path,
    DataChangeCallback dataChangeCallback
  );

  void unsubscribeDataChanges(const folly::fbstring& path);

private:
  static int ZK_TIMEOUT;

  void watcherCallback(int eventType, int state, const char* path);

  static void dataChangeCallback(
    int returnCode,
    const char* value,
    int valueLength,
    const struct Stat* stat,
    const void* data
  );

  static void watcherCallback(
    zhandle_t* zhandle,
    int eventType,
    int state,
    const char* path,
    void* watcherContext
  );

  std::unique_ptr<zhandle_t, ZHandleDeleter> zHandle_;

  toft::Mutex dataChangeCallbackMapMutex_;
  btree::btree_map<folly::fbstring, DataChangeCallback> dataChangeCallbackMap_;
};

} // namespace zk
} // namespace brood
} // namespace zerus

#endif // ZERUS_BROOD_ZK_ZK_CLIENT_H
