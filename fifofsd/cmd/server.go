// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/moisespsena-go/fifofs"
	"github.com/moisespsena-go/error-wrap"
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "serve [flags] DIR",
	Short: "Start server",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		q, err := fifofs.NewQueue(args[0])
		if err != nil {
			return errwrap.Wrap(err, "Init Queue")
		}
		addr, err := cmd.Flags().GetString("bind")
		if err != nil {
			return errwrap.Wrap(err, "Get bind flag")
		}
		srv := fifofs.QueueServer{Queue: q, Addr: addr,Log:log.Info, Error:log.Error}
		err = srv.Forever()
		if err != nil {
			return errwrap.Wrap(err, "Serve")
		}
		return err
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.PersistentFlags().StringP("bind", "b", ":6666", "The bind address")
}
