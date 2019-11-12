// Copyright 2018 Owkin, inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeterogeneousTraintuples(t *testing.T) {
	scc := new(SubstraChaincode)
	mockStub := NewMockStubWithRegisterNode("substra", scc)
	registerItem(t, *mockStub, "aggregateAlgo")

	// parent 1 (only used when child is composite)
	inpParent1 := inputCompositeTraintuple{}
	inpParent1.fillDefaults()
	resp := mockStub.MockInvoke("42", inpParent1.getArgs())
	var _key1 struct{ Key string }
	json.Unmarshal(resp.Payload, &_key1)
	parent1Key := _key1.Key

	// parent 2
	inpAggregateAlgo := inputAggregateAlgo{}
	inpAggregateAlgo.fillDefaults()
	resp = mockStub.MockInvoke("42", inpAggregateAlgo.getArgs())
	inpParent2 := inputAggregateTraintuple{}
	inpParent2.fillDefaults()
	resp = mockStub.MockInvoke("42", inpParent2.getArgs())
	var _key2 struct{ Key string }
	json.Unmarshal(resp.Payload, &_key2)
	parent2Key := _key2.Key

	// child
	inpCompositeTraintuple := inputCompositeTraintuple{}
	inpCompositeTraintuple.InHeadModelKey = parent1Key
	inpCompositeTraintuple.InTrunkModelKey = parent2Key
	inpCompositeTraintuple.fillDefaults()
	args := inpCompositeTraintuple.getArgs()
	resp = mockStub.MockInvoke("42", args)
	require.EqualValuesf(t, 200, resp.Status, "creating a %v with %s parents should succeed", CompositeTraintupleType, AggregateTraintupleType)

	// TODO (aggregate): check recursive update (fail)
	// TODO (aggregate): check recursive update (success)
}

// TODO (aggregate): test more types of parents
