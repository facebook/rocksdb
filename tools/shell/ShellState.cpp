#include <iostream>
#include <string>
#include <sstream>
#include <vector>

#include "ShellState.h"
#include "ShellContext.h"
#include "transport/TTransportException.h"

using namespace std;

using namespace apache::thrift::transport;

const char * PMT = ">> ";


void ShellStateStart::run(ShellContext * c) {
  if(!c->ParseInput()) {
    c->changeState(ShellStateStop::getInstance());
  } else {
    c->changeState(ShellStateConnecting::getInstance());
  }
}


void ShellStateStop::run(ShellContext * c) {
  c->stop();
}

void ShellStateConnecting::run(ShellContext * c) {
  try {
    c->connect();
  } catch (const TTransportException & e) {
    cout << e.what() << endl;
    c->changeState(ShellStateStop::getInstance());
    return;
  }
  
  c->changeState(ShellStateConnected::getInstance());
}

void ShellStateConnected::unknownCmd(void) {
  cout << "Unknown command!" << endl;
  cout << "Use help to list all available commands" << endl;
}

void ShellStateConnected::helpMsg(void) {
  cout << "Currently supported commands:" << endl;
  cout << "create db" << endl;
  cout << "get db key" << endl;
  cout << "scan db start_key end_key limit" << endl;
  cout << "put db key value" << endl;
  cout << "exit/quit" << endl;
}

void ShellStateConnected::handleConError(ShellContext * c) {
  cout << "Connection down" << endl;
  cout << "Reconnect ? (y/n) :" << endl;
  string s;
  while(getline(cin, s)) {
    if("y" == s) {
      c->changeState(ShellStateConnecting::getInstance());
      break;
    } else if("n" == s) {
      c->changeState(ShellStateStop::getInstance());
      break;
    } else {
      cout << "Reconnect ? (y/n) :" << endl;
    }
  }
}

void ShellStateConnected::run(ShellContext * c) {
  string line;
  cout << PMT;
  getline(cin, line);
  istringstream is(line);
  vector<string> params;
  string param;
  while(is >> param) {
    params.push_back(param);
  }

  // empty input line
  if(params.empty())
    return;

  if("quit" == params[0] || "exit" == params[0]) {
    c->changeState(ShellStateStop::getInstance());
  } else if("get" == params[0]) {
    if(params.size() == 3) {
      try {
        c->get(params[1], params[2]);
      } catch (const TTransportException & e) {
        cout << e.what() << endl;
        handleConError(c);
      }
    } else {
      unknownCmd();
    }
  } else if("create" == params[0]) {
    if(params.size() == 2) {
      try {
        c->create(params[1]);
      } catch (const TTransportException & e) {
        cout << e.what() << endl;
        handleConError(c);
      }
    } else {
      unknownCmd();
    }
  }else if("put" == params[0]) {
    if(params.size() == 4) {
      try {
        c->put(params[1], params[2], params[3]);
      } catch (const TTransportException & e) {
        cout << e.what() << endl;
        handleConError(c);
      }
    } else {
      unknownCmd();
    }
  } else if("scan" == params[0]) {
    if(params.size() == 5) {
      try {
        c->scan(params[1], params[2], params[3], params[4]);
      } catch (const TTransportException & e) {
        cout << e.what() << endl;
        handleConError(c);
      }
    } else {
      unknownCmd();
    }
  } else if("help" == params[0]) {
    helpMsg();
  } else {
    unknownCmd();
  }
}
