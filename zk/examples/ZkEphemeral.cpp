/**
 * @author Huahang Liu (huahang@zerus.co)
 * @date 2014-08-18
 */

#include "brood/zk/ZkClient.h"

#include <memory>
#include <signal.h>
#include <stdlib.h>

#include "glog/logging.h"
#include "gflags/gflags.h"

DEFINE_string(
  path,
  "/zerus/brood/zk/zk_example/pool",
  "Parent path to create ephemeral node"
);

DEFINE_string(
  node_name,
  "1",
  "Ephemeral node name"
);

DEFINE_string(
  zkHost,
  "10.237.12.2:2181",
  "Zookeeper host to connect"
);

namespace {

std::unique_ptr<zerus::brood::zk::ZkClient> zkClient;

void
InvokeDefaultSignalHandler(int signalNumber) {
  struct sigaction sigAction;
  memset(&sigAction, 0, sizeof(sigAction));
  sigemptyset(&sigAction.sa_mask);
  sigAction.sa_handler = SIG_DFL;
  sigaction(signalNumber, &sigAction, NULL);
  kill(getpid(), signalNumber);
}

void
SignalHandler(int signalNumber, siginfo_t* signalInfo, void* ucontext) {
  zkClient.reset(nullptr);
  InvokeDefaultSignalHandler(signalNumber);
}

void
InstallSignalHandler() {
  struct sigaction sigAction;
  memset(&sigAction, 0, sizeof(sigAction));
  sigemptyset(&sigAction.sa_mask);
  sigAction.sa_flags |= SA_SIGINFO;
  sigAction.sa_sigaction = &SignalHandler;
  sigaction(SIGINT, &sigAction, nullptr);
}

} // anonymous namespace

int main(int argc, char** argv) {
  using zerus::brood::zk::ZkClient;
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::SetCommandLineOption("logtostderr", "true");
  google::InitGoogleLogging(argv[0]);
  InstallSignalHandler();
  google::InstallFailureSignalHandler();
  LOG(INFO) << "PID #" << pthread_self();
  zkClient.reset(new ZkClient);
  zkClient->connect(FLAGS_zkHost);
  zkClient->createEphemeral(FLAGS_path + "/" + FLAGS_node_name, FLAGS_node_name);
  while(true) {
  }
  return 0;
}
