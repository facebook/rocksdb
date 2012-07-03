exception CtxError {
  1: string message
} (message = "message")

service ConnCtxService {
  binary getClientAddress() throws (1: CtxError error)
}
