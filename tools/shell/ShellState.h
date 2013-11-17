
#ifndef TOOLS_SHELL_SHELLSTATE
#define TOOLS_SHELL_SHELLSTATE

class ShellContext;

/*
 * Currently, there are four types of state in total
 * 1. start state: the first state the program enters
 * 2. connecting state: the program try to connect to a rocksdb server, whose
 *    previous states could be "start" or "connected" states
 * 3. connected states: the program has already connected to a server, and is
 *    processing user commands
 * 4. stop state: the last state the program enters, do some cleaning up things
 */

class ShellState {
 public:
  virtual void run(ShellContext *) = 0;
  virtual ~ShellState() {}
};


class ShellStateStart : public ShellState {
 public:
  static ShellStateStart * getInstance(void) {
    static ShellStateStart instance;
    return &instance;
  }

  virtual void run(ShellContext *);

 private:
  ShellStateStart() {}
  virtual ~ShellStateStart() {}
};

class ShellStateStop : public ShellState {
 public:
  static ShellStateStop * getInstance(void) {
    static ShellStateStop instance;
    return &instance;
  }

  virtual void run(ShellContext *);

 private:
  ShellStateStop() {}
  virtual ~ShellStateStop() {}

};

class ShellStateConnecting : public ShellState {
 public:
  static ShellStateConnecting * getInstance(void) {
    static ShellStateConnecting instance;
    return &instance;
  }

  virtual void run(ShellContext *);

 private:
  ShellStateConnecting() {}
  virtual ~ShellStateConnecting() {}

};

class ShellStateConnected : public ShellState {
 public:
  static ShellStateConnected * getInstance(void) {
    static ShellStateConnected instance;
    return &instance;
  }

  virtual void run(ShellContext *);

 private:
  ShellStateConnected() {}
  virtual ~ShellStateConnected() {}

  void unknownCmd();
  void handleConError(ShellContext *);
  void helpMsg();
};

#endif

