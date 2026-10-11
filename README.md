# control-events

*This library and system was fully designed, documented, and implemented
manually by me, a human. It is written with care.*

Delimited events for system control and monitoring, including rules and chain
reactions. An MQTT broker must be running in all machines whose processes
pub/sub events, and it also responsible for bridging events across machines.

See the haddocks of [Control.Events](https://github.com/alt-romes/control-events/blob/master/src/Control/Events.hs), the library entry point, to get started. 

The nix modules abstract the configuration of a broker for use with
`control-events`, with support for macOS (via `nix-darwin`) and NixOS.

## Testing

`cabal test` needs an MQTT broker listening on `127.0.0.1:1883`.
