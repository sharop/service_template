package main

import (
	"fmt"
	"github.com/sharop/service_template_dex/distio/agent"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"
)

// 1. Setup Log
// 2. Setup Agent register
// 3. Setup Internal services
// 4. Setup GQL Client

func main() {

	cli := &cli{}

	cmd := &cobra.Command{
		Use:     "calli",
		PreRunE: cli.setupConfig,
		RunE:    cli.run, // Call when command run. Primary logic.
	}

	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}

}

type cli struct {
	cfg cfg
}

type cfg struct {
	agent.Config
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	cmd.Flags().String("config-file", "", "Path to config file.")
	dataDir := path.Join(os.TempDir(), "calli")
	cmd.Flags().String("data-dir",
		dataDir,
		"Directory to store log and Raft data.")
	cmd.Flags().String("node-name", hostname, "Unique server ID.")
	cmd.Flags().String("bid-addr", "127.0.0.1:8401", "Addres to bind Serf on")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluester.")
	cmd.Flags().Bool("voter", false, "Voter or not voter")

	return viper.BindPFlags(cmd.Flags())
}

func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	var err error

	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	viper.SetConfigFile(configFile)
	if err = viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}
	c.cfg.DataDir = viper.GetString("data-dir")
	c.cfg.NodeName = viper.GetString("node-name")
	c.cfg.BindAddr = viper.GetString("bind-addr")
	c.cfg.RPCPort = viper.GetInt("rpc-port")
	c.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.cfg.Bootstrap = viper.GetBool("bootstrap")
	c.cfg.Voter = viper.GetBool("voter")

	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	var err error

	// Create agent
	agent, err := agent.New(c.cfg.Config)
	if err != nil {
		return err
	}

	quit := make(chan os.Signal, 1)
	// Notify Sigint, Sigterm
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	//Hey listen!!
	go func() {
		sig := <-quit
		fmt.Println()
		fmt.Printf("dEx Signal  %v", sig)
		done <- true
	}()
	fmt.Println("Process executing...")
	<-done
	log.Println("Stopping HTTP server", zap.String("reason", "received signal"))

	fmt.Println("Bye bye flaca!!")
	// Shutdown the agent.
	return agent.Shutdown()
}
