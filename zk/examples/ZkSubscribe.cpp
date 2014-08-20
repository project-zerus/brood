/**
 * @author Huahang Liu (huahang@zerus.co)
 * @date 2014-08-18
 */

#include "brood/zk/ZkClient.h"

#include "glog/logging.h"
#include "gflags/gflags.h"

DEFINE_string(
  path,
  "/zerus/brood/zk/zk_example/config/Default",
  "Path to watch"
);

DEFINE_string(
  zkHost,
  "10.237.12.2:2181",
  "Zookeeper host to connect"
);

int main(int argc, char** argv) {
  using zerus::brood::zk::ZkClient;
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::SetCommandLineOption("logtostderr", "true");
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  LOG(INFO) << "PID #" << pthread_self();
  ZkClient zkClient;
  zkClient.connect(FLAGS_zkHost);
  zkClient.subscribeDataChanges(
    FLAGS_path,
    [] (const folly::fbstring& value) {
      LOG(INFO) << "PID #" << pthread_self();
      LOG(INFO) << "data changed: " << value;
    }
  );
  while(true) {
  }
  return 0;
}
