# TODO

## Others
Complete chroot checkin of paths (see java lib sources)
Check ACL, authentication, SASL, etc
Check if packet delimitation is not too heavy or incorrect

## Request-Response implementation
getMaxChildrenRequest
checkVersionRequest
getSASLRequest
setSASLRequest
setMaxChildrenRequest
MultiHeader
AuthPacket

## Issues
Request processing without for comprehension may be related to the dispatcher, or packet delimiter
Notifications: write a custom based on mux and finagle-irc (response tagging)

## Tests
Problems while running integration tests together, ok to run separately
Write more integration tests
Fix pending integration tests
Write more unit tests(dispatcher, transporter)