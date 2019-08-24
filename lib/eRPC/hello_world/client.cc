#include "common.h"
#include <sys/time.h>

erpc::Rpc<erpc::CTransport> *rpc;
erpc::MsgBuffer req;
erpc::MsgBuffer resp;

struct timespec s,e;

bool received = false;

void cont_func(void *, void *) { clock_gettime(CLOCK_REALTIME, &e);
                                 printf("%s, latency = %.2f \n", resp.buf, (e.tv_nsec-s.tv_nsec)/1000.0);
                                 received = true; }

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

int main() {
  std::string client_uri = kClientHostname + ":" + std::to_string(kUDPPort);
  erpc::Nexus nexus(client_uri, 0, 0);

  rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, sm_handler, 1);

  std::string server_uri = kServerHostname + ":" + std::to_string(kUDPPort);
  int session_num = rpc->create_session(server_uri, 0);

  while (!rpc->is_connected(session_num)) rpc->run_event_loop_once();

  req = rpc->alloc_msg_buffer_or_die(kMsgSize);
  resp = rpc->alloc_msg_buffer_or_die(kMsgSize);

  for (int i = 0; i<1000; i++) {
      clock_gettime(CLOCK_REALTIME, &s);
      rpc->enqueue_request(session_num, kReqType, &req, &resp, cont_func, nullptr);
      while (!received) {
        rpc->run_event_loop(15);
      }
      received = false;
  }

  delete rpc;
}
