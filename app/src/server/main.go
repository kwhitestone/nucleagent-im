package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kwhitestone/prism-fusion/core"
	_ "nucleagent-im/addons"
)

func main() {
	// 与 core.RunApplication 内部同样的信号上下文；显式持有一份是为了让日志上送
	// 与应用同生命周期（收到 SIGINT/SIGTERM 时一起收敛并落盘游标）。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startLogShipping(ctx)

	if err := core.RunApplicationContext(ctx, core.ApplicationOptions{}); err != nil {
		log.Fatal(err)
	}
}
