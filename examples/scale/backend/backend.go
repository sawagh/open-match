// Copyright 2019 Google LLC
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

package backend

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"open-match.dev/open-match/examples/scale/profiles"
	"open-match.dev/open-match/examples/scale/tickets"
	"open-match.dev/open-match/internal/config"
	"open-match.dev/open-match/internal/logging"
	"open-match.dev/open-match/internal/rpc"
	"open-match.dev/open-match/pkg/pb"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "openmatch",
		"component": "scale.backend",
	})

	// TODO: Add metrics to track matches created, tickets assigned, deleted.
	matchCount uint64
	assigned   uint64
	deleted    uint64
)

// Run triggers execution of functions that continuously fetch, assign and
// delete matches.
func Run() {
	cfg, err := config.Read()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatalf("cannot read configuration.")
	}

	logging.ConfigureLogging(cfg)
	beConn, err := rpc.GRPCClientFromConfig(cfg, "api.backend")
	if err != nil {
		logger.Fatalf("failed to connect to Open Match Backend, got %v", err)
	}

	defer beConn.Close()
	be := pb.NewBackendClient(beConn)

	feConn, err := rpc.GRPCClientFromConfig(cfg, "api.frontend")
	if err != nil {
		logger.Fatalf("failed to connect to Open Match Frontend, got %v", err)
	}

	defer feConn.Close()
	fe := pb.NewFrontendClient(feConn)

	// The buffered channels attempt to decouple fetch, assign and delete. It is
	// best effort and these operations may still block each other if buffers are full.
	matches := make(chan *pb.Match, 10000)
	// deleteIds := make(chan string, 10000)
	time.Sleep(time.Second * 60)

	// go doAssign(be, matches, deleteIds)
	// go doDelete(fe, deleteIds)

	// for {
	logger.Infof("Creating Tickets")
	doCreate(fe)

	logger.Infof("Fetching Matches")
	doFetch(cfg, be, matches)
	logger.Infof("Successfully fetched Matches")
	// }
	select {}

}

func doCreate(fe pb.FrontendClient) {
	var created uint64
	var failed uint64
	start := time.Now()
	for created < 1000 {
		var wg sync.WaitGroup
		for i := 0; i < 500; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				req := &pb.CreateTicketRequest{
					Ticket: tickets.PlaylistTicket(),
				}

				if _, err := fe.CreateTicket(context.Background(), req); err != nil {
					logger.WithFields(logrus.Fields{
						"error": err.Error(),
					}).Error("failed to create a ticket.")
					atomic.AddUint64(&failed, 1)
					time.Sleep(time.Second * 1)
					return
				}

				atomic.AddUint64(&created, 1)
			}(&wg)
		}

		// Wait for all concurrent creates to complete.
		wg.Wait()
		logger.Infof("%v tickets created, %v failed in %v", created, failed, time.Since(start))
	}
}

// doFetch continuously fetches all profiles in a loop and queues up the fetched
// matches for assignment.
func doFetch(cfg config.View, be pb.BackendClient, matches chan *pb.Match) {
	startTime := time.Now()
	mprofiles := profiles.Generate(cfg)
	logger.Infof("Total number of profiles to fetch: %v", len(mprofiles))
	time.Sleep(time.Second * 1)
	var wg sync.WaitGroup
	for i, p := range mprofiles {
		wg.Add(1)
		p := p
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			fetch(be, p, matches, i)
		}(&wg, i)
	}

	// Wait for all FetchMatches calls to complete before proceeding.
	wg.Wait()
	logger.Infof("FetchedMatches:%v, AssignedTickets:%v, DeletedTickets:%v in time %v, Total profiles: %v", atomic.LoadUint64(&matchCount), atomic.LoadUint64(&assigned), atomic.LoadUint64(&deleted), time.Since(startTime), len(mprofiles))
}

func fetch(be pb.BackendClient, p *pb.MatchProfile, matches chan *pb.Match, i int) {
	for {
		failed := 0
		startTime := time.Now()
		req := &pb.FetchMatchesRequest{
			Config: &pb.FunctionConfig{
				Host: "om-function",
				Port: 50502,
				Type: pb.FunctionConfig_GRPC,
			},
			Profiles: []*pb.MatchProfile{p},
		}

		stream, err := be.FetchMatches(context.Background(), req)
		if err != nil {
			//logger.Errorf("FetchMatches for profile[%v]:%v failed, got %v", p.Name, i, err)
			failed++
			time.Sleep(time.Second * 1)
			startTime = time.Now()
			continue
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				logger.Infof("Fetched matches for profile[%v]:%v in time %v (Failed %v times)", i, p.Name, time.Since(startTime), failed)
				return
			}

			if err != nil {
				failed++
				// logger.Errorf("FetchMatches for profile[%v]:%v failed, got %v", p.Name, i, err)
				return
			}

			matches <- resp.GetMatch()
			atomic.AddUint64(&matchCount, 1)
		}
	}
}

// doAssign continuously assigns matches that were queued in the matches channel
// by doFetch and after successful assignment, queues all the tickets to deleteIds
// channel for deletion by doDelete.
func doAssign(be pb.BackendClient, matches chan *pb.Match, deleteIds chan string) {
	for match := range matches {
		ids := []string{}
		for _, t := range match.Tickets {
			ids = append(ids, t.Id)
		}

		req := &pb.AssignTicketsRequest{
			TicketIds: ids,
			Assignment: &pb.Assignment{
				Connection: fmt.Sprintf("%d.%d.%d.%d:2222", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256)),
			},
		}

		if _, err := be.AssignTickets(context.Background(), req); err != nil {
			logger.Errorf("AssignTickets failed, got %v", err)
			continue
		}

		atomic.AddUint64(&assigned, uint64(len(ids)))
		for _, id := range ids {
			deleteIds <- id
		}
	}
}

// doDelete deletes all the tickets whose ids get added to the deleteIds channel.
func doDelete(fe pb.FrontendClient, deleteIds chan string) {
	for id := range deleteIds {
		req := &pb.DeleteTicketRequest{
			TicketId: id,
		}

		if _, err := fe.DeleteTicket(context.Background(), req); err != nil {
			logger.Errorf("DeleteTicket failed for ticket %v, got %v", id, err)
			continue
		}

		atomic.AddUint64(&deleted, 1)
	}
}

func unavailable(err error) bool {
	if status.Convert(err).Code() == codes.Unavailable {
		return true
	}

	return false
}
