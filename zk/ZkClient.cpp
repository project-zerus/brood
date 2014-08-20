/**
 * @author Huahang Liu (huahang@zerus.co)
 * @date 2014-08-18
 */

#include "ZkClient.h"

#include <algorithm>

#include "glog/logging.h"
#include "gflags/gflags.h"

namespace zerus {
namespace brood {
namespace zk {

namespace {

folly::fbstring
zooStateToString(int state) {
  if (0 == state) {
    return "ZOO_CLOSED_STATE";
  } else if (ZOO_CONNECTING_STATE == state) {
    return "ZOO_CONNECTING_STATE";
  } else if (ZOO_ASSOCIATING_STATE == state) {
    return "ZOO_ASSOCIATING_STATE";
  } else if (ZOO_CONNECTED_STATE == state) {
    return "ZOO_CONNECTED_STATE";
  } else if (ZOO_EXPIRED_SESSION_STATE == state) {
    return "ZOO_EXPIRED_SESSION_STATE";
  } else if (ZOO_AUTH_FAILED_STATE == state) {
    return "ZOO_AUTH_FAILED_STATE";
  }
  return "INVALID_STATE";
}

folly::fbstring
zooWatcherEventToString(int event) {
  if (0 == event) {
    return "ZOO_ERROR_EVENT";
  } else if (ZOO_CREATED_EVENT == event) {
    return "ZOO_CREATED_EVENT";
  } else if (ZOO_DELETED_EVENT == event) {
    return "ZOO_DELETED_EVENT";
  } else if (ZOO_CHANGED_EVENT == event) {
    return "ZOO_CHANGED_EVENT";
  } else if (ZOO_CHILD_EVENT == event) {
    return "ZOO_CHILD_EVENT";
  } else if (ZOO_SESSION_EVENT == event) {
    return "ZOO_SESSION_EVENT";
  } else if (ZOO_NOTWATCHING_EVENT == event) {
    return "ZOO_NOTWATCHING_EVENT";
  }
  return "INVALID_EVENT";
}

folly::fbstring
zooErrorCodeToString(int error) {
  switch (error) {
  case ZOK:
    return "ZOK";
  case ZSYSTEMERROR:
    return "ZSYSTEMERROR";
  case ZRUNTIMEINCONSISTENCY:
    return "ZRUNTIMEINCONSISTENCY";
  case ZDATAINCONSISTENCY:
    return "ZDATAINCONSISTENCY";
  case ZCONNECTIONLOSS:
    return "ZCONNECTIONLOSS";
  case ZMARSHALLINGERROR:
    return "ZMARSHALLINGERROR";
  case ZUNIMPLEMENTED:
    return "ZUNIMPLEMENTED";
  case ZOPERATIONTIMEOUT:
    return "ZOPERATIONTIMEOUT";
  case ZBADARGUMENTS:
    return "ZBADARGUMENTS";
  case ZINVALIDSTATE:
    return "ZINVALIDSTATE";
  case ZAPIERROR:
    return "ZAPIERROR";
  case ZNONODE:
    return "ZNONODE";
  case ZNOAUTH:
    return "ZNOAUTH";
  case ZBADVERSION:
    return "ZBADVERSION";
  case ZNOCHILDRENFOREPHEMERALS:
    return "ZNOCHILDRENFOREPHEMERALS";
  case ZNODEEXISTS:
    return "ZNODEEXISTS";
  case ZNOTEMPTY:
    return "ZNOTEMPTY";
  case ZSESSIONEXPIRED:
    return "ZSESSIONEXPIRED";
  case ZINVALIDCALLBACK:
    return "ZINVALIDCALLBACK";
  case ZINVALIDACL:
    return "ZINVALIDACL";
  case ZAUTHFAILED:
    return "ZAUTHFAILED";
  case ZCLOSING:
    return "ZCLOSING";
  case ZNOTHING:
    return "ZNOTHING";
  case ZSESSIONMOVED:
    return "ZSESSIONMOVED";
  }
  return folly::fbstring("UNKNOWNERROR(") + error + ")";
}

struct DataChangeCallbackContext {
  folly::fbstring path;
  ZkClient* zkClient;
};

} // anonymous namespace

void
ZHandleDeleter::operator() (zhandle_t* zhandle) const {
  CHECK(nullptr != zhandle) << "NULL zhandle";
  LOG(INFO) << "Destroying zhandle: " << zhandle;
  int result = zookeeper_close(zhandle);
  CHECK(ZOK == result)
    << "zookeeper_close() failed with error: "
    << zooErrorCodeToString(result);
}

int ZkClient::ZK_TIMEOUT = 10000;

void
ZkClient::dataChangeCallback(
  int returnCode,
  const char* value,
  int valueLength,
  const struct Stat* stat,
  const void* data) {
  CHECK(ZOK == returnCode)
    << "dataChangeCallback() failed with error: "
    << zooErrorCodeToString(returnCode);
  CHECK(data != nullptr) << "NULL data pointer";
  const auto* context = static_cast<const DataChangeCallbackContext*>(data);
  folly::fbstring path(std::move(context->path));
  auto* zkClient = context->zkClient;
  delete context;
  {
    toft::Mutex::Locker locker(&zkClient->dataChangeCallbackMapMutex_);
    auto it = zkClient->dataChangeCallbackMap_.find(path);
    auto end = zkClient->dataChangeCallbackMap_.end();
    if (it == end) {
      LOG(ERROR) << "No callback registered at path: " << path;
      return;
    }
    it->second(folly::fbstring(value, valueLength), false);
  }
}

void
ZkClient::watcherCallback(
  zhandle_t* zhandle,
  int eventType,
  int state,
  const char* path,
  void* watcherContext) {
  ZkClient* zkClient = static_cast<ZkClient*>(watcherContext);
  CHECK(nullptr != zkClient) << "null zkClient";
  CHECK(zhandle == zkClient->zHandle_.get())
    << "Inconsistency ("
    << "zhandle: " << zhandle
    << "zhandle in zkClient: " << zkClient->zHandle_.get()
    << ")";
  zkClient->watcherCallback(eventType, state, path);
}

void
ZkClient::connect(const folly::fbstring& serverList) {
  zhandle_t* zhandle = zookeeper_init(
    serverList.c_str(),
    ZkClient::watcherCallback,
    ZK_TIMEOUT,
    nullptr,
    this,
    0
  );
  CHECK(nullptr != zhandle) << "zookeeper_init() failed";
  LOG(INFO) << "zhandle created: " << zhandle;
  zHandle_.reset(zhandle);
}

void
ZkClient::subscribeDataChanges(
  const folly::fbstring& path,
  DataChangeCallback dataChangeCallback) {
  {
    toft::Mutex::Locker locker(&dataChangeCallbackMapMutex_);
    dataChangeCallbackMap_[path] = std::move(dataChangeCallback);
  }
  DataChangeCallbackContext* context = new DataChangeCallbackContext();
  context->path = path;
  context->zkClient = this;
  zhandle_t* zhandle = zHandle_.get();
  CHECK(nullptr != zhandle) << "zhandle is NULL";
  int returnCode = zoo_aget(
    zhandle,
    path.c_str(),
    1,
    ZkClient::dataChangeCallback,
    context
  );
  CHECK(ZOK == returnCode)
    << "zoo_aget() failed with error: "
    << zooErrorCodeToString(returnCode);
}

void
ZkClient::unsubscribeDataChanges(const folly::fbstring& path) {
  toft::Mutex::Locker locker(&dataChangeCallbackMapMutex_);
  auto it = dataChangeCallbackMap_.find(path);
  auto end = dataChangeCallbackMap_.end();
  if (it == end) {
    LOG(ERROR) << "No callback registered at path: " << path;
    return;
  }
  dataChangeCallbackMap_.erase(it);
}

void
ZkClient::watcherCallback(int eventType, int state, const char* path) {
  LOG(INFO)
    << "PID #" << pthread_self() << " "
    << "ZkClient::watcherCallback("
    << "path:" << path << ","
    << "state:" << zooStateToString(state) << ","
    << "event:" << zooWatcherEventToString(eventType)
    << ")";
  if (ZOO_CONNECTED_STATE == state) {
    zhandle_t* zhandle = zHandle_.get();
    CHECK(nullptr != zhandle) << "zhandle is NULL";
    if (ZOO_CHANGED_EVENT == eventType) {
      DataChangeCallbackContext* context = new DataChangeCallbackContext();
      context->path = path;
      context->zkClient = this;
      int returnCode = zoo_aget(
        zhandle,
        path,
        1,
        ZkClient::dataChangeCallback,
        context
      );
      CHECK(ZOK == returnCode)
        << "zoo_aget() failed with code: "
        << zooErrorCodeToString(returnCode);
    } else if (ZOO_DELETED_EVENT == eventType) {
      toft::Mutex::Locker locker(&dataChangeCallbackMapMutex_);
      auto it = dataChangeCallbackMap_.find(path);
      auto end = dataChangeCallbackMap_.end();
      if (it == end) {
        LOG(ERROR) << "No callback registered at path: " << path;
        return;
      }
      it->second("", true);
      dataChangeCallbackMap_.erase(it);
    }
  } else if (ZOO_CONNECTING_STATE == state) {
  } else if (ZOO_ASSOCIATING_STATE == state) {
  } else if (ZOO_AUTH_FAILED_STATE == state) {
    CHECK(false) << "ZK Auth failed";
  } else if (ZOO_EXPIRED_SESSION_STATE == state) {
    CHECK(false) << "ZK Session expired";
  }
}

} // namespace zk
} // namespace brood
} // namespace zerus
