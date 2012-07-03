struct Message {
  1: bool a,
  2: byte b,
  3: i16 c,
  4: i32 d,
  5: i64 e,
  6: double f,
  7: string g
}

typedef list<Message> MsgList
typedef list<i64> IntList
typedef list<string> StringList


typedef map<string, Message> MsgMap
typedef set<string> MsgSet

struct Ooo {
  1: MsgList l,
  2: MsgMap m,
  3: MsgSet s
}

service DebugProtocolService {
  void Func(1: MsgList lst);
}
