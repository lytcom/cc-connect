# Integration Guide — Remote Dispatch into cc-connect Engine

## Overview

This document describes the minimal changes needed in `core/engine.go` to enable
remote agent routing. The goal is ONE line of agent substitution + switch command handling.

## Changes to core/engine.go

### 1. Add field to Engine struct

```go
// In the Engine struct definition, add:
remoteRouter *remote.Router  // nil when remote dispatch is disabled
```

### 2. Add import

```go
import "github.com/chenhg5/cc-connect/agent/remote"
```

### 3. Initialize in constructor (when config enables it)

```go
// In NewEngine() or wherever Engine is initialized:
if cfg.RemoteDispatch.Enabled {
    dispatcher := remote.NewDispatcher()
    e.remoteRouter = remote.NewRouter(dispatcher, cfg.RemoteDispatch.StateFile)
    // Start WebSocket server
    go func() {
        http.HandleFunc(cfg.RemoteDispatch.Path, dispatcher.HandleConnect)
        log.Fatal(http.ListenAndServeTLS(":"+cfg.RemoteDispatch.Port, certFile, keyFile, nil))
    }()
}
```

### 4. Handle switch commands (in handleCommand or after slash command check)

Insert after line ~2050 (after `if e.handleCommand(p, msg, content) { return }`):

```go
// Remote dispatch: handle /local, /server switch commands
if e.remoteRouter != nil {
    if handled, reply := e.remoteRouter.HandleSwitchCommand(msg.SessionKey, content); handled {
        e.reply(p, msg.ReplyCtx, reply)
        return
    }
}
```

### 5. Route to remote agent (THE KEY CHANGE)

Insert at line ~2141, just before `go e.processInteractiveMessageWith(...)`:

```go
// Remote dispatch: substitute agent if routing to personal computer
if e.remoteRouter != nil {
    if remoteAgent := e.remoteRouter.RouteMessage(msg.SessionKey, msg.UserID); remoteAgent != nil {
        agent = remoteAgent
    }
}
```

That's it. The existing `processInteractiveMessageWith` call already accepts an `agent`
parameter — we just swap it to the RemoteAgent. All streaming, event handling, and
reply mechanisms work unchanged because RemoteAgent implements core.AgentSession.

## Total invasion: ~15 lines added to engine.go

No existing lines modified, only additions.
