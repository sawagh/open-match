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

package profiles

import (
	"fmt"
	"math"

	"open-match.dev/open-match/pkg/pb"
)

var (
	regions = []string{
		"region.europe0",
		"region.europe1",
		"region.europe2",
		"region.europe3",
		"region.europe4",
		"region.europe5",
		"region.europe6",
		"region.europe7",
		"region.europe8",
		"region.europe9",
		"region.europe10",
		"region.europe11",
		"region.europe12",
		"region.europe13",
		"region.europe14",
		"region.europe15",
		"region.europe16",
		"region.europe17",
		"region.europe18",
		"region.europe19",
	}
	platforms = []string{
		"platform.pc",
		"platform.xbox",
		"platform.ps",
		"platform.nintendo",
		"platform.any",
	}
	playlists = []string{
		"mmr.playlist1",
		"mmr.playlist2",
		"mmr.playlist3",
		"mmr.playlist4",
		"mmr.playlist5",
		"mmr.playlist6",
		"mmr.playlist7",
		"mmr.playlist8",
		"mmr.playlist9",
		"mmr.playlist10",
		"mmr.playlist11",
		"mmr.playlist12",
		"mmr.playlist13",
		"mmr.playlist14",
		"mmr.playlist15",
		"mmr.playlist16",
		"mmr.playlist17",
		"mmr.playlist18",
		"mmr.playlist19",
		"mmr.playlist20",
	}
	gameSizeMap = map[string]int{
		"mmr.playlist1":  4,
		"mmr.playlist2":  4,
		"mmr.playlist3":  4,
		"mmr.playlist4":  4,
		"mmr.playlist5":  6,
		"mmr.playlist6":  6,
		"mmr.playlist7":  6,
		"mmr.playlist8":  6,
		"mmr.playlist9":  6,
		"mmr.playlist10": 6,
		"mmr.playlist11": 6,
		"mmr.playlist12": 8,
		"mmr.playlist13": 8,
		"mmr.playlist14": 8,
		"mmr.playlist15": 8,
		"mmr.playlist16": 10,
		"mmr.playlist17": 10,
		"mmr.playlist18": 10,
		"mmr.playlist19": 10,
		"mmr.playlist20": 10,
	}
)

func scaleProfiles() []*pb.MatchProfile {
	mmrRanges := makeRangeFilters(&rangeConfig{
		name:         "mmr",
		min:          0,
		max:          100,
		rangeSize:    10,
		rangeOverlap: 0,
	})

	var profiles []*pb.MatchProfile
	for _, region := range regions {
		for _, platform := range platforms {
			for _, playlist := range playlists {
				for _, mmrRange := range mmrRanges {
					poolName := fmt.Sprintf("%s_%s_%s_%v_%v", region, platform, playlist, mmrRange.min, mmrRange.max)
					p := &pb.Pool{
						Name: poolName,
						DoubleRangeFilters: []*pb.DoubleRangeFilter{
							{
								DoubleArg: region,
								Min:       0,
								Max:       math.MaxFloat64,
							},
							{
								DoubleArg: platform,
								Min:       0,
								Max:       math.MaxFloat64,
							},
							{
								DoubleArg: playlist,
								Min:       float64(mmrRange.min),
								Max:       float64(mmrRange.max),
							},
						},
					}
					prof := &pb.MatchProfile{
						Name:    fmt.Sprintf("Profile_%s", poolName),
						Pools:   []*pb.Pool{p},
						Rosters: []*pb.Roster{makeRosterSlots(p.GetName(), gameSizeMap[playlist])},
					}

					profiles = append(profiles, prof)
				}
			}
		}
	}

	return profiles
}
