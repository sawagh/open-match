/*
package apisrv provides an implementation of the gRPC server defined in ../proto/backend.proto

Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package apisrv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	backend "github.com/GoogleCloudPlatform/open-match/cmd/backendapi/proto"
	"github.com/GoogleCloudPlatform/open-match/internal/metrics"
	redisHelpers "github.com/GoogleCloudPlatform/open-match/internal/statestorage/redis"
	log "github.com/sirupsen/logrus"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"

	"github.com/tidwall/gjson"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
	"github.com/spf13/viper"

	"google.golang.org/grpc"
)

// Logrus structured logging setup
var (
	beLogFields = log.Fields{
		"app":       "openmatch",
		"component": "backend",
		"caller":    "backend/apisrv/apisrv.go",
	}
	beLog = log.WithFields(beLogFields)
)

// BackendAPI implements backend.ApiServer, the server generated by compiling
// the protobuf, by fulfilling the backend.APIClient interface.
type BackendAPI struct {
	grpc *grpc.Server
	cfg  *viper.Viper
	pool *redis.Pool
}
type backendAPI BackendAPI

// New returns an instantiated srvice
func New(cfg *viper.Viper, pool *redis.Pool) *BackendAPI {
	s := BackendAPI{
		pool: pool,
		grpc: grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{})),
		cfg:  cfg,
	}

	// Add a hook to the logger to auto-count log lines for metrics output thru OpenCensus
	log.AddHook(metrics.NewHook(BeLogLines, KeySeverity))

	backend.RegisterAPIServer(s.grpc, (*backendAPI)(&s))
	beLog.Info("Successfully registered gRPC server")
	return &s
}

// Open opens the api grpc service, starting it listening on the configured port.
func (s *BackendAPI) Open() error {
	ln, err := net.Listen("tcp", ":"+s.cfg.GetString("api.backend.port"))
	if err != nil {
		beLog.WithFields(log.Fields{
			"error": err.Error(),
			"port":  s.cfg.GetInt("api.backend.port"),
		}).Error("net.Listen() error")
		return err
	}

	beLog.WithFields(log.Fields{"port": s.cfg.GetInt("api.backend.port")}).Info("TCP net listener initialized")

	go func() {
		err := s.grpc.Serve(ln)
		if err != nil {
			beLog.WithFields(log.Fields{"error": err.Error()}).Error("gRPC serve() error")
		}
		beLog.Info("serving gRPC endpoints")
	}()

	return nil
}

// CreateMatch is this service's implementation of the CreateMatch gRPC method
// defined in ../proto/backend.proto
func (s *backendAPI) CreateMatch(c context.Context, p *backend.Profile) (*backend.MatchObject, error) {

	// Get a cancel-able context
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// Create context for tagging OpenCensus metrics.
	funcName := "CreateMatch"
	fnCtx, _ := tag.New(ctx, tag.Insert(KeyMethod, funcName))

	beLog = beLog.WithFields(log.Fields{"func": funcName})
	beLog.WithFields(log.Fields{
		"profileID": p.Id,
	}).Info("gRPC call executing")

	// Write profile
	_, err := redisHelpers.Create(ctx, s.pool, p.Id, p.Properties)
	if err != nil {
		beLog.WithFields(log.Fields{
			"error":     err.Error(),
			"component": "statestorage",
		}).Error("Statestorage failure to create match profile")

		// Failure! Return empty match object and the error
		stats.Record(fnCtx, BeGrpcErrors.M(1))
		return &backend.MatchObject{}, err
	}

	beLog.WithFields(log.Fields{
		"profileID": p.Id,
	}).Info("Profile written to statestorage")

	// Generate a request to fill the profile
	moID := strings.Replace(uuid.New().String(), "-", "", -1)
	profileRequestKey := moID + "." + p.Id

	_, err = redisHelpers.Update(ctx, s.pool, s.cfg.GetString("queues.profiles.name"), profileRequestKey)
	if err != nil {
		beLog.WithFields(log.Fields{
			"error":     err.Error(),
			"component": "statestorage",
		}).Error("Statestorage failure to queue profile")

		// Failure! Return empty match object and the error
		stats.Record(fnCtx, BeGrpcErrors.M(1))
		return &backend.MatchObject{}, err
	}

	beLog.WithFields(log.Fields{
		"profileID":         p.Id,
		"matchObjectID":     moID,
		"profileRequestKey": profileRequestKey,
	}).Info("Profile added to processing queue")

	// get and return matchobject
	watchChan := redisHelpers.Watcher(ctx, s.pool, profileRequestKey) // Watcher() runs the appropriate Redis commands.
	mo := &backend.MatchObject{Id: p.Id, Properties: ""}
	errString := ("Error retrieving matchmaking results from statestorage")
	timeout := time.Duration(s.cfg.GetInt("interval.resultsTimeout")) * time.Second

	select {
	case <-time.After(timeout):
		// TODO:Timeout: deal with the fallout.  There are some edge cases here.
		// When there is a timeout, need to send a stop to the watch channel.
		stats.Record(fnCtx, BeGrpcRequests.M(1))
		return mo, errors.New(errString + ": timeout exceeded")

	case properties, ok := <-watchChan:
		if !ok {
			// ok is false if watchChan has been closed by redisHelpers.Watcher()
			stats.Record(fnCtx, BeGrpcRequests.M(1))
			return mo, errors.New(errString + ": channel closed - was the context cancelled?")
		}

		beLog.WithFields(log.Fields{
			"profileRequestKey": profileRequestKey,
			"matchObjectID":     moID,
			// DEBUG ONLY: This prints the entire result from redis to the logs
			"matchProperties": properties, // very verbose!
		}).Debug("Received match object from statestorage")

		// 'ok' was true, so properties should contain the results from redis.
		// Do some error checking on the returned JSON
		if !gjson.Valid(properties) {
			// Just splitting this across lines for readability/wrappability
			thisError := ": Retreived json was malformed"
			thisError = thisError + " - did the evaluator write a valid JSON match object?"
			stats.Record(fnCtx, BeGrpcErrors.M(1))
			return mo, errors.New(errString + thisError)
		}

		mmfError := gjson.Get(properties, "error")
		if mmfError.Exists() {
			stats.Record(fnCtx, BeGrpcErrors.M(1))
			return mo, errors.New(errString + ": " + mmfError.String())
		}

		// Passed error checking; safe to send this property blob to the calling client.
		mo.Properties = properties
	}

	beLog.WithFields(log.Fields{
		"profileID":         p.Id,
		"matchObjectID":     moID,
		"profileRequestKey": profileRequestKey,
	}).Info("Matchmaking results received, returning to backend client")

	stats.Record(fnCtx, BeGrpcRequests.M(1))
	return mo, err
}

// ListMatches is this service's implementation of the ListMatches gRPC method
// defined in ../proto/backend.proto
// This is the steaming version of CreateMatch - continually submitting the profile to be filled
// until the requesting service ends the connection.
func (s *backendAPI) ListMatches(p *backend.Profile, matchStream backend.API_ListMatchesServer) error {

	// call creatematch in infinite loop as long as the stream is open
	ctx := matchStream.Context() // https://talks.golang.org/2015/gotham-grpc.slide#30

	// Create context for tagging OpenCensus metrics.
	funcName := "ListMatches"
	fnCtx, _ := tag.New(ctx, tag.Insert(KeyMethod, funcName))

	beLog = beLog.WithFields(log.Fields{"func": funcName})
	beLog.WithFields(log.Fields{
		"profileID": p.Id,
	}).Info("gRPC call executing. Calling CreateMatch. Looping until cancelled.")

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, probably because the client cancelled their request, time to exit.
			beLog.WithFields(log.Fields{
				"profileID": p.Id,
			}).Info("gRPC Context cancelled; client is probably finished receiving matches")

			// TODO: need to make sure that in-flight matches don't get leaked here.
			stats.Record(fnCtx, BeGrpcRequests.M(1))
			return nil

		default:
			// Retreive results from Redis
			mo, err := s.CreateMatch(ctx, p)

			beLog = beLog.WithFields(log.Fields{"func": funcName})

			if err != nil {
				beLog.WithFields(log.Fields{"error": err.Error()}).Error("Failure calling CreateMatch")
				stats.Record(fnCtx, BeGrpcErrors.M(1))
				return err
			}
			beLog.WithFields(log.Fields{"matchProperties": fmt.Sprintf("%v", &mo)}).Debug("Streaming back match object")
			matchStream.Send(mo)

			// TODO: This should be tunable, but there should be SOME sleep here, to give a requestor a window
			// to cleanly close the connection after receiving a match object when they know they don't want to
			// request any more matches.
			time.Sleep(2 * time.Second)
		}
	}
}

// DeleteMatch is this service's implementation of the DeleteMatch gRPC method
// defined in ../proto/backend.proto
func (s *backendAPI) DeleteMatch(ctx context.Context, mo *backend.MatchObject) (*backend.Result, error) {

	// Create context for tagging OpenCensus metrics.
	funcName := "DeleteMatch"
	fnCtx, _ := tag.New(ctx, tag.Insert(KeyMethod, funcName))

	beLog = beLog.WithFields(log.Fields{"func": funcName})
	beLog.WithFields(log.Fields{
		"matchObjectID": mo.Id,
	}).Info("gRPC call executing")

	_, err := redisHelpers.Delete(ctx, s.pool, mo.Id)
	if err != nil {
		beLog.WithFields(log.Fields{
			"error":     err.Error(),
			"component": "statestorage",
		}).Error("Statestorage error")

		stats.Record(fnCtx, BeGrpcErrors.M(1))
		return &backend.Result{Success: false, Error: err.Error()}, err
	}

	beLog.WithFields(log.Fields{
		"matchObjectID": mo.Id,
	}).Info("Match Object deleted.")

	stats.Record(fnCtx, BeGrpcRequests.M(1))
	return &backend.Result{Success: true, Error: ""}, err
}

// CreateAssignments is this service's implementation of the CreateAssignments gRPC method
// defined in ../proto/backend.proto
func (s *backendAPI) CreateAssignments(ctx context.Context, a *backend.Assignments) (*backend.Result, error) {

	// TODO: make playerIDs a repeated protobuf message field and iterate over it
	assignments := strings.Split(a.Roster.PlayerIds, " ")

	// Create context for tagging OpenCensus metrics.
	funcName := "CreateAssignments"
	fnCtx, _ := tag.New(ctx, tag.Insert(KeyMethod, funcName))

	beLog = beLog.WithFields(log.Fields{"func": funcName})
	beLog.WithFields(log.Fields{
		"numAssignments": len(assignments),
	}).Info("gRPC call executing")

	// TODO: relocate this redis functionality to a module
	redisConn := s.pool.Get()
	defer redisConn.Close()

	// Create player assignments in a transaction
	redisConn.Send("MULTI")
	for _, playerID := range assignments {
		beLog.WithFields(log.Fields{
			"query":                       "HSET",
			"playerID":                    playerID,
			s.cfg.GetString("connstring"): a.ConnectionInfo.ConnectionString,
		}).Debug("Statestorage operation")
		redisConn.Send("HSET", playerID, s.cfg.GetString("connstring"), a.ConnectionInfo.ConnectionString)
	}
	_, err := redisConn.Do("EXEC")

	// Issue encountered
	if err != nil {
		beLog.WithFields(log.Fields{
			"error":     err.Error(),
			"component": "statestorage",
		}).Error("Statestorage error")

		stats.Record(fnCtx, BeGrpcErrors.M(1))
		stats.Record(fnCtx, BeAssignmentFailures.M(int64(len(assignments))))
		return &backend.Result{Success: false, Error: err.Error()}, err
	}

	// Success!
	beLog.WithFields(log.Fields{
		"numAssignments": len(assignments),
	}).Info("Assignments complete")

	stats.Record(fnCtx, BeGrpcRequests.M(1))
	stats.Record(fnCtx, BeAssignments.M(int64(len(assignments))))
	return &backend.Result{Success: true, Error: ""}, err
}

// DeleteAssignments is this service's implementation of the DeleteAssignments gRPC method
// defined in ../proto/backend.proto
func (s *backendAPI) DeleteAssignments(ctx context.Context, a *backend.Roster) (*backend.Result, error) {
	// TODO: make playerIDs a repeated protobuf message field and iterate over it
	assignments := strings.Split(a.PlayerIds, " ")

	// Create context for tagging OpenCensus metrics.
	funcName := "DeleteAssignments"
	fnCtx, _ := tag.New(ctx, tag.Insert(KeyMethod, funcName))

	beLog = beLog.WithFields(log.Fields{"func": funcName})
	beLog.WithFields(log.Fields{
		"numAssignments": len(assignments),
	}).Info("gRPC call executing")

	// TODO: relocate this redis functionality to a module
	redisConn := s.pool.Get()
	defer redisConn.Close()

	// Remove player assignments in a transaction
	redisConn.Send("MULTI")
	// TODO: make playerIDs a repeated protobuf message field and iterate over it
	for _, playerID := range assignments {
		beLog.WithFields(log.Fields{"query": "DEL", "key": playerID}).Debug("Statestorage operation")
		redisConn.Send("DEL", playerID)
	}
	_, err := redisConn.Do("EXEC")

	// Issue encountered
	if err != nil {
		beLog.WithFields(log.Fields{
			"error":     err.Error(),
			"component": "statestorage",
		}).Error("Statestorage error")

		stats.Record(fnCtx, BeGrpcErrors.M(1))
		stats.Record(fnCtx, BeAssignmentDeletionFailures.M(int64(len(assignments))))
		return &backend.Result{Success: false, Error: err.Error()}, err
	}

	// Success!
	stats.Record(fnCtx, BeGrpcRequests.M(1))
	stats.Record(fnCtx, BeAssignmentDeletions.M(int64(len(assignments))))
	return &backend.Result{Success: true, Error: ""}, err
}
